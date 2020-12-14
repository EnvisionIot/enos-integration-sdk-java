import com.envisioniot.enos.iot_http_integration.FileCategory;
import com.envisioniot.enos.iot_http_integration.HttpConnection;
import com.envisioniot.enos.iot_http_integration.message.IFileCallback;
import com.envisioniot.enos.iot_mqtt_sdk.core.exception.EnvisionException;
import com.envisioniot.enos.sdk.data.DeviceInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author :charlescai
 * @date :2020-03-16
 */
public class DownloadFileSample {
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

    public static void main(String[] args) {
        // Construct a http connection
        HttpConnection connection = new HttpConnection.Builder(
                INTEGRATION_CHANNEL_URL, API_GW_URL, APP_KEY, APP_SECRET, ORG_ID)
                .build();

        DeviceInfo deviceInfo = new DeviceInfo().setAssetId(ASSET_ID);
        // fileUri is an enos scheme file uri
        String fileUri = "enos-connect://xxx.txt";

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            InputStream inputStream = connection.downloadFile(deviceInfo, fileUri, FileCategory.FEATURE);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, len);
            }
            byte[] data = outputStream.toByteArray();
            System.out.println(new String(data));
        } catch (EnvisionException | IOException e) {
            e.printStackTrace();
        }

        // Asynchronously call the file download request
        try {
            connection.downloadFile(deviceInfo, fileUri, FileCategory.FEATURE, new IFileCallback() {
                        @Override
                        public void onResponse(InputStream inputStream) throws IOException {
                            System.out.println("download file asynchronously");
                            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = inputStream.read(buffer)) != -1) {
                                outputStream.write(buffer, 0 ,len);
                            }
                            byte[] data = outputStream.toByteArray();
                            System.out.println(new String(data));
                        }

                        @Override
                        public void onFailure(Exception failure) {
                            failure.printStackTrace();
                        }
                    }
            );
        } catch (EnvisionException e) {
            e.printStackTrace();
        }
    }
}
