package stephen.webflux.snowflake.service.impl;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import stephen.webflux.snowflake.common.exception.IdGenerationException;
import stephen.webflux.snowflake.service.IdService;

/**
 * 基于Zookeeper的ID Service
 * 通过ZK管理WorkerID
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "snowflake.zookeeper.enable", havingValue = "true")
public class ZkIdService extends IdService {
    /**
     * 启动zookeeper版本 id service
     *
     * @param dataFile
     */
    public ZkIdService(@Value("${snowflake.dataFile}") String dataFile) {
        setDataFile(dataFile);

        // 当前时间发生回跳，终止程序启动
        JSONObject data = readData();
        long lastTimestamp = data.getLong("lastTimestamp");
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp < lastTimestamp) {
            throw new IdGenerationException("Clock has moved backward,current time:" + currentTimestamp + ", last time:" + lastTimestamp);
        }
        setLastTimestamp(lastTimestamp);

        // 获取workerId
        setWorkerId(initWorkId());

        // 启动异步刷新数据
        asyncFlush();
    }

    @Override
    public void asyncFlush() {

    }

    private Long initWorkId() {
        // todo
        return new Long(12);
    }
}
