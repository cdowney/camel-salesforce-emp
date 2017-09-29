package io.cad.client.salesforce.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cad.client.http.JettyHttpClient;
import io.cad.config.ConnectionConfig;
import org.apache.camel.support.ServiceSupport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.MimeTypes;

import java.net.URL;
import java.util.concurrent.TimeUnit;

public class SalesforceClient extends ServiceSupport {

    private static final String COMETD_PATH = "/cometd/";
    private static final String OAUTH_TOKEN_PATH = "/services/oauth2/token";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ConnectionConfig config;
    private final HttpClient client;

    private AuthResponse authResponse;

    public SalesforceClient(ConnectionConfig config, JettyHttpClient jettyHttpClient) {
        this.config = config;
        this.client = jettyHttpClient.getHttpClient();
    }

    public URL getCometdEndpoint() {
        try {
            return new URL(authResponse.getInstanceUrl() + COMETD_PATH + config.getApiVersion());
        } catch (Throwable t) {
            throw new RuntimeException("Error building endpoint", t);
        }
    }

    public String getBearerToken() {
        return authResponse.getAccessToken();
    }

    private synchronized void updateSalesforceToken() {
        authResponse = getAuthResponse();
        if (config.getEnableKeepAlive()) {
            client.getScheduler().schedule(this::updateSalesforceToken, config.getKeepAliveMinutes(), TimeUnit.MINUTES);
        }
    }

    private synchronized AuthResponse getAuthResponse() {

        try {
            ContentResponse response = client.newRequest(config.getLoginUrl() + OAUTH_TOKEN_PATH)
                    .method(HttpMethod.POST)
                    .header(HttpHeader.CONTENT_TYPE, MimeTypes.Type.FORM_ENCODED.asString())
                    .param("grant_type", "password")
                    .param("client_id", config.getClientId())
                    .param("client_secret", config.getClientSecret())
                    .param("username", config.getUserName())
                    .param("password", config.getPassword() + config.getSecurityToken())
                    .send();
            return objectMapper.readValue(response.getContentAsString(), AuthResponse.class);
        } catch (Throwable t) {
            // TODO: Retry/Log/Throw?
            throw new RuntimeException("Failed to get AuthResponse from Salesforce");
        }

    }

    @Override
    public void doStart() throws Exception {
        updateSalesforceToken();
    }

    @Override
    public void doStop() throws Exception {
    }
}
