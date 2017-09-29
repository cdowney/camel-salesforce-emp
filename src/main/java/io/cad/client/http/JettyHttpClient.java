package io.cad.client.http;

import org.apache.camel.support.ServiceSupport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class JettyHttpClient extends ServiceSupport {

    private HttpClient httpClient;

    public JettyHttpClient() {
        httpClient = new HttpClient(new SslContextFactory());
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    protected void doStart() throws Exception {
        httpClient.start();
    }

    @Override
    protected void doStop() throws Exception {
        httpClient.stop();
    }
}
