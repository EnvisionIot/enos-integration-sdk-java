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
 * @date :2020-02-20
 */
public class IntegrationEventPostRequest extends BaseIntegrationRequest {
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getRequestAction() {
        return RequestAction.POST_EVENT_ACTION;
    }

    public static class Builder extends BaseBuilder<IntegrationEventPostRequest> {

        private Map<Pair<DeviceInfo/*deviceInfo*/, Long/*time*/>, Map<String/*eventId*/, Map<String, Object>>> events;

        Builder() {
            this.events = new LinkedHashMap<>();
        }

        public Builder addEvent(DeviceInfo deviceInfo, long time, Map<String/*eventId*/, Map<String, Object>> eventValues) {
            Map<String, Map<String, Object>> map = this.events.computeIfAbsent(
                    Pair.makePair(deviceInfo, time), pair -> new HashMap<>());
            map.putAll(eventValues);
            return this;
        }

        @Override
        protected Object createParams() {
            List<Map<String, Object>> params = new ArrayList<>();
            if (events != null) {
                for (Map.Entry<Pair<DeviceInfo, Long>, Map<String/*eventId*/, Map<String, Object>>> entry : events.entrySet()) {
                    Map<String, Object> param = new HashMap<>();
                    DeviceInfo deviceInfo = entry.getKey().first;
                    if (StringUtil.isNotEmpty(deviceInfo.getAssetId())) {
                        param.put("assetId", deviceInfo.getAssetId());
                    } else {
                        param.put("productKey", deviceInfo.getProductKey());
                        param.put("deviceKey", deviceInfo.getDeviceKey());
                    }
                    param.put("time", entry.getKey().second);
                    param.put("events", entry.getValue());
                    params.add(param);
                }
            }
            return params;
        }

        void fileCheck(DeviceInfo deviceInfo, Map<String/*eventId*/, Map<String, Object>> eventMap) {
            for (Map.Entry<String, Map<String, Object>> entry : eventMap.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> value = entry.getValue();
                if (value != null)
                {
                    HashMap<String, Object> replicaMap = Maps.newHashMap();
                    for (Map.Entry<String, Object> subEntry : value.entrySet())
                    {
                        if (subEntry.getValue() instanceof File) {
                            // store sub-value as file
                            String fileUri = LOCAL_FILE_SCHEMA + storeFile(deviceInfo, FeatureType.EVENT, key, ((File) subEntry.getValue()));
                            replicaMap.put(subEntry.getKey(), fileUri);
                        } else {
                            replicaMap.put(subEntry.getKey(), subEntry.getValue());
                        }
                    }
                    eventMap.put(key, replicaMap);
                }
            }
        }

        @Override
        protected String createMethod() {
            return MethodConstants.INTEGRATION_EVENT_POST;
        }

        @Override
        public IntegrationEventPostRequest build() {
            events.forEach((key, value) -> fileCheck(key.first, value));
            IntegrationEventPostRequest request = super.build();
            request.setFiles(this.files);
            return request;
        }

        @Override
        protected IntegrationEventPostRequest createRequestInstance() {
            return new IntegrationEventPostRequest();
        }
    }

}
