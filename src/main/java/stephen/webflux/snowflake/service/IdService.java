package stephen.webflux.snowflake.service;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import stephen.webflux.snowflake.common.exception.IdGenerationException;

import java.io.*;
import java.util.Random;

@Getter
@Setter
@Accessors(chain = true)
@Slf4j
public abstract class IdService {
    /**
     * WorkerID
     */
    private volatile Long workerId;


    /**
     * 存储数据的文件
     */
    private volatile String dataFile;

    /**
     * 最后一次生成ID的timestamp，单位ms
     */
    private volatile Long lastTimestamp = System.currentTimeMillis();

    /**
     * 最后一次同毫秒序列号
     */
    private volatile Long lastSequenceNum = 0L;

    /**
     * 同毫秒序列号的bit数量
     */
    protected static final Long SEQUENCE_NUM_BITS = 12L;

    /**
     * workerId的bit数量
     */
    protected static final Long WORKER_ID_BITS = 10L;

    /**
     * 时间戳所占bit数量
     */
    protected static final Long TIMESTAMP_BITS = 41L;

    /**
     * Server启动的年代时间
     */
    protected static final Long TIMESTAMP_EPOCH = 1546272000000L;

    /**
     * 最大的同毫秒序列号
     */
    protected static final Long MAX_SEQUENCE_NUM = ~(-1L << SEQUENCE_NUM_BITS);

    /**
     * 最大的workerID
     */
    protected static final Long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    /**
     * Server启动的年代时间
     */
    protected static final Long MAX_TIMESTAMP = TIMESTAMP_EPOCH + ~(-1L << TIMESTAMP_BITS);


    /**
     * 生成ID
     *
     * @return Long
     */
    public synchronized Long next() {
        long currentTimestamp = System.currentTimeMillis();
        long workerId = getWorkerId();

        long lastTimestamp = getLastTimestamp();
        long lastSequenceNum = getLastSequenceNum();
        long sequenceNum = lastSequenceNum;

        /*
         * 1.判断时间窗是否已用完
         * 2.判断时间回跳
         * 3.查看当前毫秒的序列号是否用完，如果用完了，sleep(1ms)
         * 4.序列号++
         * 5.进行位操作拼接ID
         */
        if (currentTimestamp > MAX_TIMESTAMP) {
            throw new IdGenerationException("The current time has exceeded the maximum time");
        }

        if (currentTimestamp < lastTimestamp) {
            /*
             * 参考ntpd文档http://doc.ntp.org/4.1.0/ntpd.htm
             * 渐变(slew)：
             * 如果误差在128ms以内, 则ntpd会对系统时钟进行渐变(slew), 直到和ntp server一致为止,
             * 此时系统时钟会略微变快或者变慢, 但依然是单调增加的, 所以对于snowflake来说是安全的.
             * 渐变时, 每一秒最多变慢或者变慢0.5ms; 所以如果误差为128ms, 则最少需要256s才能调整过来.
             *
             * 跳变(step)：
             * 如果超过128ms, 就会进行跳变(step), 此时时间是不连续的, 如果往后调, snowflake就要出错.
             * 一般在ntp服务刚启动的时候, 误差较大, 此时可能误差几秒到几十个小时;
             *  如果BIOS断电后重启, 则误差可能几十年. 此时ntp服务必然会进行跳变, 则snowflake出错就是大概率事件.
             *
             * 渐变不用考虑，对于向后跳变，直接报错即可
             * 一般来说除了人为拨动时钟，其他的拨动都是闰秒，所以建议将ntpd的渐变阈值调整为2S
             */
            throw new IdGenerationException("Clock has moved backward");
        } else if (currentTimestamp == lastTimestamp) {
            if (lastSequenceNum >= MAX_SEQUENCE_NUM) {
                try {
                    // sleep 1ms，重试
                    Thread.sleep(1);
                    return next();
                } catch (InterruptedException e) {
                    throw new IdGenerationException(e);
                }
            }
        } else {
            // 时间大于上次时间，序列号重置成0-100的随机整数（避免hash取模分布倾斜），必须小于MAX_SEQUENCE_NUM
            sequenceNum = new Random().nextInt(100);
        }


        sequenceNum++;

        setLastSequenceNum(sequenceNum);
        setLastTimestamp(currentTimestamp);

        Long id = ((currentTimestamp - TIMESTAMP_EPOCH) << (WORKER_ID_BITS + SEQUENCE_NUM_BITS))
                | (workerId << WORKER_ID_BITS)
                | sequenceNum;

        log.info(id + "|" + currentTimestamp + "|" + workerId + "|" + sequenceNum);

        return id;
    }

    /**
     * 异步进行数据落盘或者落到其他存储介质
     */
    public abstract void asyncFlush();

    /**
     * 读取数据
     *
     * @return Json
     */
    protected JSONObject readData() {
        File file = new File(getDataFile());
        if (!file.exists()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("lastTimestamp", System.currentTimeMillis());
            return jsonObject;
        } else {
            Long fileLength = file.length();
            byte[] fileContent = new byte[fileLength.intValue()];
            try {
                FileInputStream in = new FileInputStream(file);
                in.read(fileContent);
                in.close();
            } catch (IOException e) {
                throw new IdGenerationException(e);
            }
            String jsonString = new String(fileContent);


            return JSONObject.parseObject(jsonString);
        }
    }
}
