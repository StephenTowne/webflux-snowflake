package stephen.webflux.snowflake.common.response;

/**
 * 请求返回结果生成辅助类
 */
public class ResponseResultGenerator {
    private static final String DEFAULT_SUCCESS_MESSAGE = "SUCCESS";
    private static final String DEFAULT_FAIL_MESSAGE = "服务器发生异常，请稍后再试";

    private static final int CODE_SUCCESS = 0;
    private static final int CODE_FAIL = 1;

    public static <T> ResponseResult<T> genSuccessResult(T data) {
        return new ResponseResult<T>()
                .setCode(CODE_SUCCESS)
                .setMsg(DEFAULT_SUCCESS_MESSAGE)
                .setData(data);
    }

    public static ResponseResult genSuccessResult() {
        return new ResponseResult<>()
                .setCode(CODE_SUCCESS)
                .setMsg(DEFAULT_SUCCESS_MESSAGE);
    }

    public static ResponseResult genFailResult() {
        return new ResponseResult<>()
                .setCode(CODE_FAIL)
                .setMsg(DEFAULT_FAIL_MESSAGE);
    }

    public static ResponseResult genFailResult(String msg) {
        return new ResponseResult<>()
                .setCode(CODE_FAIL)
                .setMsg(msg);
    }
}
