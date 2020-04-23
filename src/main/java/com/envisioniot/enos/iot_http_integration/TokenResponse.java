package com.envisioniot.enos.iot_http_integration;

import lombok.Data;

/**
 * @author :charlescai
 * @date :2020-02-26
 */
@Data
public class TokenResponse {
    private int status;
    private String msg;
    private String business;
    private ResponseData data;

    @Data
    public static class ResponseData {
        private String accessToken;
        private int expire;
    }
}
