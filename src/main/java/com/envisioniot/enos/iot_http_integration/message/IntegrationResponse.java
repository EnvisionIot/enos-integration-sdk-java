package com.envisioniot.enos.iot_http_integration.message;

import com.envisioniot.enos.iot_mqtt_sdk.util.ExactValue;
import lombok.Data;


/**
 * @author :charlescai
 * @date :2020-02-19
 */
@Data
public class IntegrationResponse {
    
    public static final int SUCCESS_CODE = 0;
    
    private int code;
    private String msg;
    private String requestId;
    private IntegrationData data;
    
    public boolean isSuccess()
    {
        return code == SUCCESS_CODE;
    }
}
