package com.envisioniot.enos.iot_http_integration.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * @author mengyuantan
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UriInfo implements Serializable {
    String fileUri;
    String uploadUrl;
    String filename;
    Map<String, String> headers;
}
