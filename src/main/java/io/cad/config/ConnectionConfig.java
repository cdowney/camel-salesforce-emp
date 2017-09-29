package io.cad.config;

import lombok.Data;

@Data
public class ConnectionConfig {

    public static final String DEFAULT_LOGIN_URL = "https://login.salesforce.com";

    private String loginUrl;
    private String clientId;
    private String clientSecret;
    private String userName;
    private String password;
    private String securityToken;
    private String apiVersion = "40.0";
    private Boolean enableKeepAlive = true;
    private Long keepAliveMinutes = 60L;

}
