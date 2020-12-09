import com.envisioniot.enos.iot_http_integration.FileCategory;
import com.envisioniot.enos.iot_http_integration.HttpConnection;
import com.envisioniot.enos.iot_mqtt_sdk.core.exception.EnvisionException;
import com.envisioniot.enos.sdk.data.DeviceInfo;

/**
 * @author mengyuantan
 */
public class DownloadFileByLarkSample {
    // EnOS Token Server URL and HTTP Broker URL, which can be obtained from Environment Information page in EnOS Console
    static final String TOKEN_SERVER_URL = "http://token_server_url";
    static final String BROKER_URL = "http://broker_url";

    // EnOS Application AccessKey and SecretKey, which can be obtain in Application Registration page in EnOS Console
    static final String APP_KEY = "appKey";
    static final String APP_SECRET = "appSecret";

    // Device credentials, which can be obtained from Device Details page in EnOS Console
    static final String ORG_ID = "orgId";
    static final String ASSET_ID = "assetId";
    static final String PRODUCT_KEY = "productKey";
    static final String DEVICE_KEY = "deviceKey";

    public static void main(String[] args) throws EnvisionException {
        // Construct a http connection
        HttpConnection connection = new HttpConnection.Builder(
                BROKER_URL, TOKEN_SERVER_URL, APP_KEY, APP_SECRET, ORG_ID)
                .build();

        DeviceInfo deviceInfo = new DeviceInfo().setAssetId(ASSET_ID);
        // fileUri is an enos scheme file uri
        String fileUri = "enos-lark://xxx.txt";

        String url = connection.getDownloadUrl(deviceInfo, fileUri, FileCategory.FEATURE);
        System.out.println(url);
    }
}