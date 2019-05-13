package stephen.webflux.snowflake.common;

import java.util.UUID;

/**
 * 线程安全的全局变量
 */
public class GlobalVar {
    private static final ThreadLocal<String> requestId = new ThreadLocal<String>() {
        @Override
        protected synchronized String initialValue() {
            return UUID.randomUUID().toString();
        }
    };

    public static String getRequestId() {
        return requestId.get();
    }

    public static void removeRequestId() {
        requestId.remove();
    }
}
