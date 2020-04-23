package com.envisioniot.enos.iot_http_integration.progress;

/**
 * Used to monitor the progress of HTTP requests.
 * @author shenjieyuan
 */
public interface IProgressListener
{
    public void onRequestProgress(long bytesWritten, long contentLength);
}
