package com.envisioniot.enos.iot_http_integration.message;

/**
 * @author :charlescai
 * @date :2020-03-11
 */
public interface IIntegrationCallback {

    /**
     * handle the async response of the integration request
     * @param response
     */
    void onResponse(IntegrationResponse response);

    /**
     * Handle exception we hit while waiting for the response
     * @param failure
     */
    void onFailure(Exception failure);

}
