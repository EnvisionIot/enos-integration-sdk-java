package com.envisioniot.enos.iot_http_integration.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author mengyuantan
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntegrationData {
    List<UriInfo> uriInfoList;

    public void addUriInfo(UriInfo uriInfo) {
        uriInfoList.add(uriInfo);
    }
}
