package com.envisioniot.enos.iot_http_integration.message;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author :charlescai
 * @date :2020-03-11
 */
public interface IFileCallback {
    /**
     * handle the async response of the file download request
     * @param inputStream
     * @throws IOException
     */
    void onResponse(InputStream inputStream) throws IOException;

    /**
     * Handle exception we hit while waiting for the response
     * @param failure
     */
    void onFailure(Exception failure);
}
