package stephen.webflux.snowflake.common.exception;

public class IdGenerationException extends RuntimeException {
    public IdGenerationException() {
    }

    public IdGenerationException(String message) {
        super(message);
    }

    public IdGenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    public IdGenerationException(Throwable cause) {
        super(cause);
    }

    public IdGenerationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
