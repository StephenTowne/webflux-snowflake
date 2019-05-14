package stephen.webflux.snowflake.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import stephen.webflux.snowflake.common.exception.IdGenerationException;
import stephen.webflux.snowflake.service.IdService;

import java.util.List;

/**
 * 基于Zookeeper的ID Service
 * 通过ZK管理WorkerID
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "snowflake.zookeeper.enable", havingValue = "true")
public class ZkIdService extends IdService {
    private volatile CuratorFramework client;
    private static final String ZK_ADDRESS = "xxx:8001";
    private static final String ZK_PATH = "/test/java";

    /**
     * 启动zookeeper版本 id service
     *
     * @param dataFile
     */
    public ZkIdService(@Value("${snowflake.dataFile}") String dataFile) {
        client = CuratorFrameworkFactory.newClient(
                ZK_ADDRESS,
                new RetryNTimes(10, 5000)
        );
        client.start();

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
        try {
            long sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
            for (long workerId = 0; workerId <= MAX_WORKER_ID; workerId++) {
                String workerIdStr = String.valueOf(workerId);
                String key = ZK_PATH + "/" + workerIdStr;
                List<String> childPaths = client.getChildren().forPath(ZK_PATH);
                // 检查workerId是否有被占用。如果未被占用则新建
                if (!childPaths.contains(workerIdStr)) {
                    client.create().
                            creatingParentsIfNeeded().
                            forPath(key, JSON.toJSONBytes(sessionId));
                    return workerId;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            client.close();
        }

        return 12L;
    }


}
