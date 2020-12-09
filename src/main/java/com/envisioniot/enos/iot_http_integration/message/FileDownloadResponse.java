package com.envisioniot.enos.iot_http_integration.message;

import com.envisioniot.enos.iot_mqtt_sdk.core.msg.IMqttResponse;

import java.util.regex.Pattern;

public class FileDownloadResponse {

    public static final int SUCCESS_CODE = 0;

    private int requestId;
    private int code;
    private String msg;
    private String data;


    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public boolean isSuccess()
    {
        return code == SUCCESS_CODE;
    }

}
