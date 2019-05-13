package stephen.webflux.snowflake.service.impl;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import stephen.webflux.snowflake.common.exception.IdGenerationException;
import stephen.webflux.snowflake.service.IdService;

import java.io.*;
import java.util.concurrent.*;

/**
 * 单机ID Service
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "snowflake.zookeeper.enable", havingValue = "false")
public class LocalIdService extends IdService {
    /**
     * 启动local id service
     *
     * @param workerId
     * @param dataFile
     */
    public LocalIdService(@Value("${snowflake.workerId}") Long workerId, @Value("${snowflake.dataFile}") String dataFile) {
        setWorkerId(workerId);
        setDataFile(dataFile);

        // 当前时间发生回跳，终止程序启动
        JSONObject data = readData();
        long lastTimestamp = data.getLong("lastTimestamp");
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp < lastTimestamp) {
            throw new IdGenerationException("Clock has moved backward,current time:" + currentTimestamp + ", last time:" + lastTimestamp);
        }
        setLastTimestamp(lastTimestamp);

        // 启动异步刷新数据
        asyncFlush();
    }

    @Override
    public void asyncFlush() {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    JSONObject data = new JSONObject();
                    data.put("lastTimestamp", getLastTimestamp());
                    OutputStream os = new FileOutputStream(getDataFile());
                    byte[] bytes = data.toJSONString().getBytes();
                    os.write(bytes);
                    os.close();
                    log.info("write data file");
                } catch (IOException e) {
                    throw new IdGenerationException(e);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
}
