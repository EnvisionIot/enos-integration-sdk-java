package com.envisioniot.enos.iot_http_integration;

import com.envisioniot.enos.iot_mqtt_sdk.core.exception.EnvisionException;
import com.envisioniot.enos.iot_mqtt_sdk.core.internals.SignMethod;
import com.envisioniot.enos.iot_mqtt_sdk.util.GsonUtil;
import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.net.SocketException;
import java.util.Calendar;

import static com.envisioniot.enos.iot_http_integration.HttpConnectionError.CLIENT_ERROR;
import static com.envisioniot.enos.iot_http_integration.HttpConnectionError.SOCKET_ERROR;
import static com.envisioniot.enos.iot_http_integration.HttpConnectionError.UNSUCCESSFUL_AUTH;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.SECOND;

/**
 * @author :charlescai
 * @date :2020-02-26
 */
@Slf4j
@Data
@Builder
public class TokenConnection {

    private static final String TOKEN_GET_PATH = "/apim-token-service/v2.0/token/get";
    private static final String TOKEN_REFRESH_PATH = "/apim-token-service/v2.0/token/refresh";

    @NonNull
    private String tokenServerUrl;

    @NonNull
    private String appKey;

    @NonNull
    private String appSecret;

    private String accessToken;
    
    private Calendar expireTime;

    private Calendar refreshTime;

    @NonNull
    private OkHttpClient okHttpClient;

    public void getAndRefreshToken() throws EnvisionException {
        getToken();
        refreshToken();
    }

    /**
     * Apply for a token
     * @return
     * @throws EnvisionException
     */
    public void getToken() throws EnvisionException {
        String timestamp = String.valueOf(System.currentTimeMillis());

        String encryption = sign(timestamp);

        GetTokenRequestBody getTokenRequestBody = new GetTokenRequestBody(appKey, encryption, timestamp);

        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), GsonUtil.toJson(getTokenRequestBody));

        Request httpRequest = new Request.Builder()
                .url(tokenServerUrl + TOKEN_GET_PATH)
                .post(requestBody)
                .build();

        Call call = okHttpClient.newCall(httpRequest);
        doExecuteCall(call);
    }

    
    /**
     * Refresh current token
     * @return
     * @throws EnvisionException
     */
    public void refreshToken() throws EnvisionException {
        String timestamp = String.valueOf(System.currentTimeMillis());

        String encryption = sign(timestamp);

        RefreshTokenRequestBody refreshTokenRequestBody = new RefreshTokenRequestBody(appKey, encryption, timestamp, accessToken);
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), GsonUtil.toJson(refreshTokenRequestBody));

        Request httpRequest = new Request.Builder()
                .url(tokenServerUrl + TOKEN_REFRESH_PATH)
                .post(requestBody)
                .build();

        Call call = okHttpClient.newCall(httpRequest);
        doExecuteCall(call);
    }
    
    
    /**
     * If needs apply for a token
     * @return
     */
    public boolean needGetToken()
    {
        return Strings.isNullOrEmpty(accessToken) ||
                Calendar.getInstance().after(expireTime);
    }
    
    /**
     * If need refresh token
     */
    public boolean needRefreshToken()
    {
        return !Strings.isNullOrEmpty(accessToken) &&
                Calendar.getInstance().after(refreshTime) && Calendar.getInstance().before(expireTime);
    }

    
    /**
     * Execute token service call and store token, expire time
     * @param call
     * @throws EnvisionException
     */
    private void doExecuteCall(Call call) throws EnvisionException {
        try {
            Response httpResponse = call.execute();
            try {
                checkArgument(httpResponse != null && httpResponse.body() != null);
                byte[] payload = httpResponse.body().bytes();
                String msg = new String(payload, UTF_8);
                
                TokenResponse response = GsonUtil.fromJson(msg, TokenResponse.class);
                
                checkArgument(response != null && response.getStatus() == 0 && 
                              response.getData() != null, "response %s", response);

                // store token and expire time, refresh time
                accessToken = response.getData().getAccessToken();
                expireTime = Calendar.getInstance();
                expireTime.add(SECOND, response.getData().getExpire());

                refreshTime = Calendar.getInstance();
                refreshTime.add(SECOND, response.getData().getExpire());
                // refresh token 5 minutes before expire
                refreshTime.add(MINUTE, -10);

            } catch (Exception e) {
                log.warn("failed to decode token get response: " + httpResponse, e);
                throw new EnvisionException(UNSUCCESSFUL_AUTH, e.getMessage());
            }
        } catch (SocketException e) {
            log.info("failed to execute request due to socket error {}", e.getMessage());
            throw new EnvisionException(SOCKET_ERROR, e.getMessage());
        } catch (Exception e) {
            log.warn("failed to execute request", e);
            throw new EnvisionException(CLIENT_ERROR, e.getMessage());
        }
    }

    private String sign(String timestamp) {
        String content =
                appKey +
                timestamp +
                appSecret;

        return SignMethod.SHA256.sign(content);
    }

    @Data
    @AllArgsConstructor
    private static class GetTokenRequestBody {
        private String appKey;
        private String encryption;
        private String timestamp;
    }

    @Data
    @AllArgsConstructor
    private static class RefreshTokenRequestBody {
        private String appKey;
        private String encryption;
        private String timestamp;

        private String accessToken;
    }

}
