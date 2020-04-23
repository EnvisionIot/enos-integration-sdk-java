package com.envisioniot.enos.iot_http_integration.message;

import com.envisioniot.enos.iot_http_integration.FileFormData;
import com.envisioniot.enos.iot_mqtt_sdk.message.upstream.tsl.UploadFileInfo;
import com.envisioniot.enos.iot_mqtt_sdk.util.ExactValue;
import com.envisioniot.enos.iot_mqtt_sdk.util.FileUtil;
import com.envisioniot.enos.iot_mqtt_sdk.util.GsonUtil;
import com.envisioniot.enos.iot_mqtt_sdk.util.StringUtil;
import com.envisioniot.enos.sdk.data.DeviceInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author :charlescai
 * @date :2020-02-19
 */
@Data
public abstract class BaseIntegrationRequest {
    private String id;
    private String method;
    private String version;
    private ExactValue params;

    private List<UploadFileInfo> files;

    /**
     * action parameter of the request API
     * @return
     */
    public abstract String getRequestAction();

    public byte[] encode() throws IOException {
        return GsonUtil.toJson(getJsonPayload()).getBytes();
    }

    private Map<String, Object> getJsonPayload() throws IOException {
        Map<String, Object> payload = new HashMap<>();
        if (getId() != null) {
            payload.put("id", getId());
        }
        if (getVersion() != null) {
            payload.put("version", getVersion());
        }
        if (getMethod() != null) {
            payload.put("method", getMethod());
        }
        if (getParams() != null) {
            payload.put("params", getParams());
        }
        if (getFiles() != null) {
            payload.put("files", CreateFilePayload().get());
        }
        return payload;
    }

    private ExactValue CreateFilePayload() throws IOException {
        Map<String, Object> disposition = Maps.newHashMap();
        for (UploadFileInfo fileInfo : files) {
            Map<String, String> map = Maps.newHashMap();
            map.put("featureId", fileInfo.getFeatureId());
            if (StringUtil.isNotEmpty(fileInfo.getAssetId())) {
                map.put("assetId", fileInfo.getAssetId());
            } else {
                map.put("productKey", fileInfo.getProductKey());
                map.put("deviceKey", fileInfo.getDeviceKey());
            }
            map.put("md5", FileFormData.md5(fileInfo.getFile()));

            disposition.put(fileInfo.getFilename(), map);
        }
        return new ExactValue(disposition);
    }

    protected abstract static class BaseBuilder<R extends BaseIntegrationRequest> {
        final String LOCAL_FILE_SCHEMA = "local://";
        List<UploadFileInfo> files = Lists.newArrayList();

        protected abstract String createMethod();

        protected abstract Object createParams();

        protected abstract R createRequestInstance();

        public R build() {
            R request = createRequestInstance();
            request.setMethod(createMethod());
            request.setParams(createParams());
            return request;
        }

        String storeFile(DeviceInfo deviceInfo, String featureType, String featureId, File file) {
            UploadFileInfo fileInfo = new UploadFileInfo();
            String filename = FileUtil.generateFileName(file);
            fileInfo.setFilename(filename);
            fileInfo.setFile(file);
            fileInfo.setFeatureType(featureType);
            fileInfo.setFeatureId(featureId);
            fileInfo.setAssetId(deviceInfo.getAssetId());
            fileInfo.setProductKey(deviceInfo.getProductKey());
            fileInfo.setDeviceKey(deviceInfo.getDeviceKey());

            files.add(fileInfo);
            return filename;
        }

    }

    public void setParams(Object params) {
        this.params = new ExactValue(params);
    }

    @SuppressWarnings("unchecked")
    public <T> T getParams() {
        return params == null ? null : (T) params.get();
    }
}
