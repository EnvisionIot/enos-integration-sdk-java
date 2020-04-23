package com.envisioniot.enos.iot_http_integration.message;

import com.envisioniot.enos.iot_mqtt_sdk.core.internals.constants.FeatureType;
import com.envisioniot.enos.iot_mqtt_sdk.core.internals.constants.MethodConstants;
import com.envisioniot.enos.iot_mqtt_sdk.util.StringUtil;
import com.envisioniot.enos.sdk.data.DeviceInfo;
import com.google.common.collect.Maps;

import java.io.File;
import java.util.*;

/**
 * @author :charlescai
 * @date :2020-02-20
 */
public class IntegrationAttributePostRequest extends BaseIntegrationRequest{
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getRequestAction() {
        return RequestAction.POST_ATTRIBUTE_ACTION;
    }

    public static class Builder extends BaseBuilder<IntegrationAttributePostRequest> {

        private Map<DeviceInfo, Map<String, Object>> attributes;

        Builder() {
            this.attributes = new LinkedHashMap<>();
        }

        public Builder addAttribute(DeviceInfo deviceInfo, Map<String, Object> attributeValue) {
            Map<String, Object> map = this.attributes.computeIfAbsent(
                    deviceInfo, pair -> new HashMap<>());
            map.putAll(attributeValue);
            return this;
        }

        @Override
        protected Object createParams() {
            List<Map<String, Object>> params = new ArrayList<>();
            if (attributes != null) {
                for (Map.Entry<DeviceInfo, Map<String, Object>> entry : attributes.entrySet()) {
                    Map<String, Object> param = new HashMap<>();
                    DeviceInfo deviceInfo = entry.getKey();
                    if (StringUtil.isNotEmpty(deviceInfo.getAssetId())) {
                        param.put("assetId", deviceInfo.getAssetId());
                    } else {
                        param.put("productKey", deviceInfo.getProductKey());
                        param.put("deviceKey", deviceInfo.getDeviceKey());
                    }
                    param.put("attributes", entry.getValue());
                    params.add(param);
                }
            }
            return params;
        }

        @SuppressWarnings("unchecked")
        void fileCheck(DeviceInfo deviceInfo, Map<String, Object> attributeMap) {
            for (Map.Entry<String, Object> entry : attributeMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof File)
                {
                    // store value as file
                    String fileUri = LOCAL_FILE_SCHEMA + storeFile(deviceInfo, FeatureType.ATTRIBUTE, key, (File) value);
                    attributeMap.put(key, fileUri);
                }
                else if (value instanceof Map)
                {
                    HashMap<String, Object> replicaMap = Maps.newHashMap();
                    for (Map.Entry<String,Object> subEntry: ((Map<String,Object>) value).entrySet())
                    {
                        if (subEntry.getValue() instanceof File) {
                            // store sub-value as file
                            String fileUri = LOCAL_FILE_SCHEMA + storeFile(deviceInfo, FeatureType.ATTRIBUTE, key, ((File) subEntry.getValue()));
                            replicaMap.put(subEntry.getKey(), fileUri);
                        } else {
                            replicaMap.put(subEntry.getKey(), subEntry.getValue());
                        }
                    }
                    attributeMap.put(key, replicaMap);
                }
            }
        }

        @Override
        protected String createMethod() {
            return MethodConstants.INTEGRATION_ATTRIBUTE_POST;
        }

        @Override
        public IntegrationAttributePostRequest build() {
            attributes.forEach(this::fileCheck);
            IntegrationAttributePostRequest request = super.build();
            request.setFiles(this.files);
            return request;
        }

        @Override
        protected IntegrationAttributePostRequest createRequestInstance() {
            return new IntegrationAttributePostRequest();
        }
    }

}
