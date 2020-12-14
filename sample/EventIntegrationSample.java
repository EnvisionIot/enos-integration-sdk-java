import com.envisioniot.enos.iot_http_integration.HttpConnection;
import com.envisioniot.enos.iot_http_integration.message.IIntegrationCallback;
import com.envisioniot.enos.iot_http_integration.message.IntegrationEventPostRequest;
import com.envisioniot.enos.iot_http_integration.message.IntegrationResponse;
import com.envisioniot.enos.iot_mqtt_sdk.core.exception.EnvisionException;
import com.envisioniot.enos.sdk.data.DeviceInfo;
import com.google.common.collect.Maps;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class EventIntegrationSample {
    // EnOS API Gateway URL and HTTP Integration Channel URL, which can be obtained from Environment Information page in EnOS Console
    static final String API_GW_URL = "http://api_gw_url";
    static final String INTEGRATION_CHANNEL_URL = "http://integration_channel_url";

    // EnOS Application AccessKey and SecretKey, which can be obtain in Application Registration page in EnOS Console
    static final String APP_KEY = "appKey";
    static final String APP_SECRET = "appSecret";

    // Device credentials, which can be obtained from Device Details page in EnOS Console
    static final String ORG_ID = "orgId";
    static final String ASSET_ID = "assetId";
    static final String PRODUCT_KEY = "productKey";
    static final String DEVICE_KEY = "deviceKey";

    private static IntegrationEventPostRequest buildEventPostRequest() {
        DeviceInfo deviceInfo1 = new DeviceInfo().setAssetId(ASSET_ID);
        DeviceInfo deviceInfo2 = new DeviceInfo().setKey(PRODUCT_KEY, DEVICE_KEY);

        // Events are defined in ThingModel
        Map<String, Map<String, Object>> hashMap = Maps.newHashMap();

        Map<String, Object> structMap = Maps.newHashMap();
        structMap.put("struct1", new File("sample1.txt"));
        structMap.put("struct2", 1234);

        hashMap.put("IntEvent1", structMap);
        return IntegrationEventPostRequest.builder()
                .addEvent(deviceInfo1, System.currentTimeMillis(), hashMap)
                .addEvent(deviceInfo2, System.currentTimeMillis(), hashMap)
                .build();
    }

    public static void main(String[] args) {
        // Construct a http connection
        HttpConnection connection = new HttpConnection.Builder(
                INTEGRATION_CHANNEL_URL, API_GW_URL, APP_KEY, APP_SECRET, ORG_ID)
                .build();

        IntegrationEventPostRequest request = buildEventPostRequest();

        try {
            IntegrationResponse response = connection.publish(request,  (bytes, length) ->
                    System.out.println(String.format("Progress: %.2f %%", (float) bytes / length * 100.0)));
            System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(response));
        } catch (EnvisionException | IOException e) {
            e.printStackTrace();
        }

        // Asynchronously call the event post with file
        request = buildEventPostRequest();

        try {
            connection.publish(request, new IIntegrationCallback() {
                        @Override
                        public void onResponse(IntegrationResponse response) {
                            System.out.println("receive response asynchronously");
                            System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(response));
                        }

                        @Override
                        public void onFailure(Exception failure) {
                            failure.printStackTrace();
                        }
                    }, (bytes, length) ->
                            System.out.println(String.format("Progress: %.2f %%", (float) bytes / length * 100.0))
            );
        } catch (IOException | EnvisionException e) {
            e.printStackTrace();
        }
    }
}
