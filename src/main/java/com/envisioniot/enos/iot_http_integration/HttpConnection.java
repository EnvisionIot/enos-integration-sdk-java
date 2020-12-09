package com.envisioniot.enos.iot_http_integration;

import com.envisioniot.enos.iot_http_integration.message.*;
import com.envisioniot.enos.iot_http_integration.progress.IProgressListener;
import com.envisioniot.enos.iot_http_integration.progress.ProgressRequestWrapper;
import com.envisioniot.enos.iot_http_integration.utils.FileUtil;
import com.envisioniot.enos.iot_mqtt_sdk.core.exception.EnvisionException;
import com.envisioniot.enos.iot_mqtt_sdk.message.upstream.tsl.UploadFileInfo;
import com.envisioniot.enos.iot_mqtt_sdk.util.GsonUtil;
import com.envisioniot.enos.iot_mqtt_sdk.util.StringUtil;
import com.envisioniot.enos.sdk.data.DeviceInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Monitor;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.envisioniot.enos.iot_http_integration.HttpConnectionError.*;
import static com.envisioniot.enos.iot_mqtt_sdk.core.internals.constants.FormDataConstants.ENOS_MESSAGE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author :charlescai
 * @date :2020-02-18
 */
@Slf4j
public class HttpConnection {
    private static final String VERSION = "1.1";

    private static final String INTEGRATION_PATH = "/connect-service/v2.1/integration";
    private static final String FILES_PATH = "/connect-service/v2.1/files";

    private static final String APIM_ACCESS_TOKEN = "apim-accesstoken";

    /**
     * Builder for http connection. A customized OkHttpClient can be provided, to
     * define specific connection pool, proxy etc. Find more at
     * {@link #okHttpClient}
     *
     * @author cai.huang
     */
    @Data
    public static class Builder {
        @NonNull
        private String integrationBrokerUrl;

        @NonNull
        private String tokenServerUrl;

        @NonNull
        private String appKey;

        @NonNull
        private String appSecret;

        @NonNull
        private String orgId;

        private OkHttpClient okHttpClient;

        private boolean useLark = false;

        private boolean autoUpload = true;

        public HttpConnection build() {
            HttpConnection instance = new HttpConnection();

            instance.integrationBrokerUrl = integrationBrokerUrl;

            instance.orgId = orgId;

            // allocate client
            if (okHttpClient == null) {
                okHttpClient = new OkHttpClient.Builder().connectTimeout(10L, TimeUnit.SECONDS)
                        .readTimeout(2L, TimeUnit.MINUTES).writeTimeout(2L, TimeUnit.MINUTES)
                        .retryOnConnectionFailure(false).build();
            }
            instance.okHttpClient = okHttpClient;

            // construct token connection
            instance.tokenConnection = TokenConnection.builder().tokenServerUrl(tokenServerUrl).appKey(appKey)
                    .appSecret(appSecret).okHttpClient(instance.okHttpClient).build();

            // initiate token
            CompletableFuture.runAsync(() ->
            {
                try {
                    instance.tokenConnection.getAndRefreshToken();
                } catch (EnvisionException e) {
                    // do nothing, already handled
                }
            });

            instance.setAutoUpload(this.autoUpload);
            instance.setUseLark(this.useLark);

            return instance;
        }

        public Builder setUseLark(boolean useLark) {
            this.useLark = useLark;
            return this;
        }

        public Builder setAutoUpload(boolean autoUpload) {
            this.autoUpload = autoUpload;
            return this;
        }
    }

    private String integrationBrokerUrl;

    private String orgId;

    // For automatic apply for access token
    private Monitor authMonitor = new Monitor();

    private TokenConnection tokenConnection;

    @Getter
    private OkHttpClient okHttpClient = null;

    @Getter
    @Setter
    private boolean autoUpload = true;

    @Getter
    @Setter
    private boolean useLark = false;

    /**
     * A sequence ID used in request
     */
    @Getter
    @Setter
    private AtomicInteger seqId = new AtomicInteger(0);

    // ======== auth via token server will be automatically executed =========

    private void checkAuth() throws EnvisionException {
        if (tokenConnection.needGetToken() || tokenConnection.needRefreshToken()) {
            auth();
        }
    }

    /**
     * Ensure to get / refresh access token
     *
     * @throws EnvisionException with code {@code UNSUCCESSFUL_AUTH} if failed to
     *                           get access token
     */
    public void auth() throws EnvisionException {
        if (authMonitor.tryEnter()) {
            try {
                // if there is no accessToken, you need to get token
                // or if token is near to expiry, you need to refresh token
                if (tokenConnection.needGetToken()) {
                    tokenConnection.getToken();
                } else if (tokenConnection.needRefreshToken()) {
                    tokenConnection.refreshToken();
                }
            } finally {
                authMonitor.leave();
            }
        } else if (authMonitor.enter(10L, TimeUnit.SECONDS)) {
            // Wait at most 10 seconds and try to get Access Token
            try {
                if (tokenConnection.needGetToken()) {
                    throw new EnvisionException(UNSUCCESSFUL_AUTH);
                }
            } finally {
                authMonitor.leave();
            }
        }
    }

    // =======================================================================

    /**
     * Publish a request to EnOS IOT HTTP broker
     * <p>
     * Response
     *
     * @param request
     * @param progressListener used to handle file uploading progress, {@code null} if not
     *                         available
     * @return response
     * @throws EnvisionException
     * @throws IOException
     */
    public IntegrationResponse publish(BaseIntegrationRequest request, IProgressListener progressListener)
            throws EnvisionException, IOException {
        Call call = generatePublishCall(request, request.getFiles(), progressListener);
        IntegrationResponse integrationResponse = publishCall(call);
        if (this.isUseLark() && request.getFiles() != null) {
            uploadFileByUrl(request, integrationResponse);
        }
        return integrationResponse;
    }

    /**
     * Publish a request to EnOS IOT HTTP broker, asynchronously with a callback
     *
     * @param request
     * @param callback
     * @param progressListener used to handle file uploading progress, {@code null} if not
     *                         available
     * @throws IOException
     * @throws EnvisionException
     */
    public void publish(BaseIntegrationRequest request, IIntegrationCallback callback,
                        IProgressListener progressListener) throws IOException, EnvisionException {
        Call call = generatePublishCall(request, request.getFiles(), progressListener);
        publishCallAsync(call, callback);
    }


    /**
     * Delete a file
     *
     * @param deviceInfo
     * @param fileUri
     * @return
     * @throws EnvisionException
     */
    public IntegrationResponse deleteFile(DeviceInfo deviceInfo, String fileUri) throws EnvisionException {
        Call call = generateDeleteCall(orgId, deviceInfo, fileUri);
        return publishCall(call);
    }

    /**
     * Delete a file async
     *
     * @param deviceInfo
     * @param fileUri
     * @param callback
     * @throws EnvisionException
     */
    public void deleteFile(DeviceInfo deviceInfo, String fileUri, IIntegrationCallback callback) throws EnvisionException {
        Call call = generateDeleteCall(orgId, deviceInfo, fileUri);
        publishCallAsync(call, callback);
    }

    /**
     * Download file
     *
     * @param deviceInfo
     * @param fileUri
     * @return
     * @throws EnvisionException
     */
    public InputStream downloadFile(DeviceInfo deviceInfo, String fileUri, FileCategory category) throws EnvisionException, IOException {

        if (fileUri.startsWith(FileScheme.ENOS_LARK_URI_SCHEME)) {
            String downloadUrl = getDownloadUrl(deviceInfo, fileUri, category);
            Response response =  FileUtil.downloadFile(downloadUrl);
            Preconditions.checkArgument(response.isSuccessful(),
                    "fail to download file, downloadUrl: %s, msg: %s",
                    downloadUrl, response.message());
            Preconditions.checkNotNull(response.body(),
                    "response body is null, downloadUrl: %s", downloadUrl);
            return response.body().byteStream();
        }

        Call call = generateDownloadCall(orgId, deviceInfo, fileUri, category);
        Response httpResponse;
        try {
            httpResponse = call.execute();

            if (!httpResponse.isSuccessful()) {
                throw new EnvisionException(httpResponse.code(), httpResponse.message());
            }

            try {
                Preconditions.checkNotNull(httpResponse);
                Preconditions.checkNotNull(httpResponse.body());

                return httpResponse.body().byteStream();
            } catch (Exception e) {
                log.info("failed to get response: " + httpResponse, e);
                throw new EnvisionException(CLIENT_ERROR);
            }
        } catch (SocketException e) {
            log.info("failed to execute request due to socket error {}", e.getMessage());
            throw new EnvisionException(SOCKET_ERROR, e.getMessage());
        } catch (EnvisionException e) {
            throw e;
        } catch (Exception e) {
            log.warn("failed to execute request", e);
            throw new EnvisionException(CLIENT_ERROR);
        }
    }

    public void downloadFile(DeviceInfo deviceInfo, String fileUri, FileCategory category, IFileCallback callback) throws EnvisionException {
        Call call;
        if (fileUri.startsWith(FileScheme.ENOS_LARK_URI_SCHEME)) {
            call = generateGetDownloadUrlCall(deviceInfo, fileUri, category);
        } else {
            call = generateDownloadCall(orgId, deviceInfo, fileUri, category);
        }


        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                callback.onFailure(e);
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (!response.isSuccessful()) {
                    callback.onFailure(new EnvisionException(response.code(), response.message()));
                }

                try {
                    Preconditions.checkNotNull(response);
                    Preconditions.checkNotNull(response.body());
                    if (response.isSuccessful() && fileUri.startsWith(FileScheme.ENOS_LARK_URI_SCHEME)) {
                        FileDownloadResponse fileDownloadResponse = GsonUtil.fromJson(
                                response.body().string(), FileDownloadResponse.class);
                        String fileDownloadUrl = fileDownloadResponse.getData();
                        response = FileUtil.downloadFile(fileDownloadUrl);
                    }
                    if (response.isSuccessful() && response.body() != null) {
                        callback.onResponse(response.body().byteStream());
                    }
                } catch (Exception e) {
                    log.info("failed to get response: " + response, e);
                    callback.onFailure(new EnvisionException(CLIENT_ERROR));
                }
            }
        });

    }

    public String getDownloadUrl(DeviceInfo deviceInfo, String fileUri, FileCategory category) throws EnvisionException {
        Call call = generateGetDownloadUrlCall(deviceInfo, fileUri, category);

        try {
            Response httpResponse = call.execute();

            Preconditions.checkNotNull(httpResponse);
            Preconditions.checkNotNull(httpResponse.body());

            FileDownloadResponse response = GsonUtil.fromJson(httpResponse.body().string(), FileDownloadResponse.class);
            if (!response.isSuccess()) {
                throw new EnvisionException(response.getCode(), response.getMsg());
            }
            return response.getData();
        } catch (SocketException e) {
            log.info("failed to execute request due to socket error {}", e.getMessage());
            throw new EnvisionException(SOCKET_ERROR, e.getMessage());
        } catch (EnvisionException e) {
            throw e;
        } catch (Exception e) {
            log.warn("failed to execute request", e);
            throw new EnvisionException(CLIENT_ERROR);
        }
    }

    /**
     * complete a Request message
     */
    private void fillRequest(BaseIntegrationRequest request) {
        if (Strings.isNullOrEmpty(request.getId())) {
            request.setId(String.valueOf(seqId.incrementAndGet()));
        }

        // Also populate request version for http
        request.setVersion(VERSION);
    }

    /**
     * Execute okHttp Call, Get Response
     *
     * @param call
     * @return
     * @throws EnvisionException
     */
    private IntegrationResponse publishCall(Call call) throws EnvisionException {
        Response httpResponse;
        try {
            httpResponse = call.execute();
            if (!httpResponse.isSuccessful()) {
                throw new EnvisionException(httpResponse.code(), httpResponse.message());
            }

            try {
                Preconditions.checkNotNull(httpResponse);
                Preconditions.checkNotNull(httpResponse.body());
                byte[] payload = httpResponse.body().bytes();
                String msg = new String(payload, UTF_8);
                return GsonUtil.fromJson(msg, IntegrationResponse.class);
            } catch (Exception e) {
                log.info("failed to decode response: " + httpResponse, e);
                throw new EnvisionException(CLIENT_ERROR);
            }
        } catch (SocketException e) {
            log.info("failed to execute request due to socket error {}", e.getMessage());
            throw new EnvisionException(SOCKET_ERROR, e.getMessage());
        } catch (EnvisionException e) {
            throw e;
        } catch (Exception e) {
            log.warn("failed to execute request", e);
            throw new EnvisionException(CLIENT_ERROR);
        }
    }

    private void uploadFileByUrl(BaseIntegrationRequest request, IntegrationResponse response) {
        List<UriInfo> uriInfos = new ArrayList<>();
        if (response.getData() != null) {
            uriInfos = response.getData().getUriInfoList();
        }
        List<UploadFileInfo> fileInfos = request.getFiles();

        Map<String, File> featureIdAndFileMap = new HashMap<>();
        fileInfos.forEach(fileInfo -> featureIdAndFileMap.put(fileInfo.getFilename(), fileInfo.getFile()));
        uriInfos.forEach(uriInfo -> {
            try {
                String filename = uriInfo.getFilename();
                uriInfo.setFilename(featureIdAndFileMap.get(filename).getName());
                if (autoUpload) {
                    Response uploadFileRsp = FileUtil.uploadFile(uriInfo.getUploadUrl(), featureIdAndFileMap.get(filename), uriInfo.getHeaders());
                    if (!uploadFileRsp.isSuccessful()) {
                        log.error("Fail to upload file automatically, filename: {}, uploadUrl: {}, msg: {}",
                                featureIdAndFileMap.get(filename).getName(),
                                uriInfo.getUploadUrl(),
                                uploadFileRsp.message());
                    }
                }
            } catch (Exception e) {
                log.error("Fail to upload file, uri info: {}, exception: {}", featureIdAndFileMap, e);
            }
        });
    }


    /**
     * Async execute okHttp Call，use Callback method to process Response
     *
     * @param call
     * @param callback
     */
    private void publishCallAsync(Call call, IIntegrationCallback callback) {

        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                callback.onFailure(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                if (!response.isSuccessful()) {
                    callback.onFailure(new EnvisionException(response.code(), response.message()));
                }
                try {
                    Preconditions.checkNotNull(response);
                    Preconditions.checkNotNull(response.body());
                    byte[] payload = response.body().bytes();
                    String msg = new String(payload, UTF_8);
                    callback.onResponse(GsonUtil.fromJson(msg, IntegrationResponse.class));
                } catch (Exception e) {
                    log.info("failed to decode response: " + response, e);
                    callback.onFailure(new EnvisionException(CLIENT_ERROR));
                }
            }
        });
    }

    private Call generatePublishCall(BaseIntegrationRequest request, List<UploadFileInfo> files,
                                     IProgressListener progressListener) throws IOException, EnvisionException {
        // ensure access token is gotten
        checkAuth();

        // 将请求消息设置完整
        fillRequest(request);

        // 准备一个Multipart请求消息
        MultipartBody.Builder builder = new MultipartBody.Builder().setType(MultipartBody.FORM)
                .addFormDataPart(ENOS_MESSAGE, new String(request.encode(), UTF_8));

        if (files != null && !useLark) {
            for (UploadFileInfo uploadFile : files) {
                builder.addPart(FileFormData.createFormData(uploadFile));
            }
        }

        RequestBody body;
        if (progressListener == null) {
            body = builder.build();
        } else {
            body = new ProgressRequestWrapper(builder.build(), progressListener);
        }

        Request httpRequest = new Request.Builder().url(
                integrationBrokerUrl + INTEGRATION_PATH + "?action=" + request.getRequestAction() + "&orgId=" + orgId + useLarkPart())
                .addHeader(APIM_ACCESS_TOKEN, tokenConnection.getAccessToken()).post(body).build();

        return okHttpClient.newCall(httpRequest);
    }

    private String useLarkPart() {
        return this.isUseLark()? "&useLark=" + true : "";
    }

    private Call generateDeleteCall(String orgId, DeviceInfo deviceInfo, String fileUri) throws EnvisionException {
        checkAuth();

        StringBuilder uriBuilder = new StringBuilder()
                .append(integrationBrokerUrl)
                .append(FILES_PATH)
                .append("?action=").append(RequestAction.DELETE_ACTION)
                .append("&orgId=").append(orgId)
                .append("&fileUri=").append(fileUri);
        if (StringUtil.isNotEmpty(deviceInfo.getAssetId())) {
            uriBuilder.append("&assetId=").append(deviceInfo.getAssetId());
        } else {
            uriBuilder.append("&productKey=").append(deviceInfo.getProductKey())
                    .append("&deviceKey=").append(deviceInfo.getDeviceKey());
        }

        Request httpRequest = new Request.Builder()
                .url(uriBuilder.toString())
                .addHeader(APIM_ACCESS_TOKEN, tokenConnection.getAccessToken())
                .post(RequestBody.create(null, ""))
                .build();

        return okHttpClient.newCall(httpRequest);
    }

    private Call generateGetDownloadUrlCall(DeviceInfo deviceInfo, String fileUri, FileCategory category) throws EnvisionException {
        checkAuth();

        StringBuilder uriBuilder = new StringBuilder()
                .append(integrationBrokerUrl)
                .append(FILES_PATH)
                .append("?action=").append(RequestAction.GET_DOWNLOAD_URL_ACTION)
                .append("&orgId=").append(orgId)
                .append("&fileUri=").append(fileUri)
                .append("&category=").append(category.getName());
        if (StringUtil.isNotEmpty(deviceInfo.getAssetId())) {
            uriBuilder.append("&assetId=").append(deviceInfo.getAssetId());
        } else {
            uriBuilder.append("&productKey=").append(deviceInfo.getProductKey())
                    .append("&deviceKey=").append(deviceInfo.getDeviceKey());
        }

        Request httpRequest = new Request.Builder()
                .url(uriBuilder.toString())
                .addHeader(APIM_ACCESS_TOKEN, tokenConnection.getAccessToken())
                .get()
                .build();

        return okHttpClient.newCall(httpRequest);
    }

    private Call generateDownloadCall(String orgId, DeviceInfo deviceInfo, String fileUri, FileCategory category) throws EnvisionException {

        checkAuth();

        StringBuilder uriBuilder = new StringBuilder()
                .append(integrationBrokerUrl)
                .append(FILES_PATH)
                .append("?action=").append(RequestAction.DOWNLOAD_ACTION)
                .append("&orgId=").append(orgId)
                .append("&fileUri=").append(fileUri)
                .append("&category=").append(category.getName());
        if (StringUtil.isNotEmpty(deviceInfo.getAssetId())) {
            uriBuilder.append("&assetId=").append(deviceInfo.getAssetId());
        } else {
            uriBuilder.append("&productKey=").append(deviceInfo.getProductKey())
                    .append("&deviceKey=").append(deviceInfo.getDeviceKey());
        }

        Request httpRequest = new Request.Builder()
                .url(uriBuilder.toString())
                .addHeader(APIM_ACCESS_TOKEN, tokenConnection.getAccessToken())
                .get()
                .build();

        return okHttpClient.newCall(httpRequest);
    }

}
