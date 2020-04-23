package com.envisioniot.enos.iot_http_integration.message;

import com.envisioniot.enos.iot_mqtt_sdk.core.internals.constants.FeatureType;
import com.envisioniot.enos.iot_mqtt_sdk.core.internals.constants.MethodConstants;
import com.envisioniot.enos.iot_mqtt_sdk.util.Pair;
import com.envisioniot.enos.iot_mqtt_sdk.util.StringUtil;
import com.envisioniot.enos.sdk.data.DeviceInfo;
import com.google.common.collect.Maps;

import java.io.File;
import java.util.*;

/**
 * @author :charlescai
 * @date :2020-02-18
 */
public class IntegrationMeasurepointPostRequest extends BaseIntegrationRequest {
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getRequestAction() {
        return RequestAction.POST_MEASUREPOINT_ACTION;
    }


    public static class Builder extends BaseBuilder<IntegrationMeasurepointPostRequest> {

        private Map<Pair<DeviceInfo/*deviceInfo*/, Long/*time*/>, Map<String/*pointId*/, Object/*value*/>> measurepoints;

        Builder() {
            this.measurepoints = new LinkedHashMap<>();
        }

        public Builder addMeasurepoint(DeviceInfo deviceInfo, long time, Map<String, Object> measurepointValues) {
            Map<String, Object> map = this.measurepoints.computeIfAbsent(
                    Pair.makePair(deviceInfo, time), pair -> new HashMap<>());
            map.putAll(measurepointValues);
            return this;
        }

        @Override
        protected Object createParams() {
            List<Map<String, Object>> params = new ArrayList<>();
            if (measurepoints != null) {
                for (Map.Entry<Pair<DeviceInfo, Long>, Map<String, Object>> entry : measurepoints.entrySet()) {
                    Map<String, Object> param = new HashMap<>();
                    DeviceInfo deviceInfo = entry.getKey().first;
                    if (StringUtil.isNotEmpty(deviceInfo.getAssetId())) {
                        param.put("assetId", deviceInfo.getAssetId());
                    } else {
                        param.put("productKey", deviceInfo.getProductKey());
                        param.put("deviceKey", deviceInfo.getDeviceKey());
                    }
                    param.put("time", entry.getKey().second);
                    param.put("measurepoints", entry.getValue());
                    params.add(param);
                }
            }
            return params;
        }

        @SuppressWarnings("unchecked")
        private void fileCheck(DeviceInfo deviceInfo, Map<String, Object> measurepointMap) {
            for (Map.Entry<String, Object> entry : measurepointMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof File)
                {
                    // store value as file
                    String fileUri = LOCAL_FILE_SCHEMA + storeFile(deviceInfo, FeatureType.MEASUREPOINT, key, (File) value);
                    measurepointMap.put(key, fileUri);
                }
                else if (value instanceof Map)
                {
                    HashMap<String, Object> replicaMap = Maps.newHashMap();
                    for (Map.Entry<String,Object> subEntry: ((Map<String,Object>) value).entrySet())
                    {
                        if (subEntry.getValue() instanceof File) {
                            // store sub-value as file
                            String fileUri = LOCAL_FILE_SCHEMA + storeFile(deviceInfo, FeatureType.MEASUREPOINT, key, ((File) subEntry.getValue()));
                            replicaMap.put(subEntry.getKey(), fileUri);
                        } else {
                            replicaMap.put(subEntry.getKey(), subEntry.getValue());
                        }
                    }
                    measurepointMap.put(key, replicaMap);
                }
            }
        }

        @Override
        protected String createMethod() {
            return MethodConstants.INTEGRATION_MEASUREPOINT_POST;
        }

        @Override
        public IntegrationMeasurepointPostRequest build() {
            measurepoints.forEach((key, value) -> fileCheck(key.first, value));
            IntegrationMeasurepointPostRequest request = super.build();
            request.setFiles(this.files);
            return request;
        }

        @Override
        protected IntegrationMeasurepointPostRequest createRequestInstance() {
            return new IntegrationMeasurepointPostRequest();
        }
    }

}
