package stephen.webflux.snowflake.common.response;


import stephen.webflux.snowflake.common.GlobalVar;

public class ResponseResult<T> {
    private String requestId = GlobalVar.getRequestId();
    private int code;
    private String msg;
    private T data;

    public int getCode() {
        return code;
    }

    public ResponseResult<T> setCode(int code) {
        this.code = code;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public ResponseResult<T> setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public T getData() {
        return data;
    }

    public ResponseResult<T> setData(T data) {
        this.data = data;
        return this;
    }

    public String getRequestId() {
        return requestId;
    }

    public ResponseResult<T> setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }
}
