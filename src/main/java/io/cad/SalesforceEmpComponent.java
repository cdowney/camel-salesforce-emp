package io.cad;

import io.cad.client.salesforce.cometd.CometDClient;
import io.cad.client.http.JettyHttpClient;
import io.cad.client.salesforce.auth.SalesforceClient;
import io.cad.config.ConnectionConfig;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelException;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.util.ServiceHelper;

import java.util.Map;

/**
 * Represents the component that manages {@link SalesforceEmpEndpoint}.
 */
public class SalesforceEmpComponent extends DefaultComponent /* implements VerifiableComponent, SSLContextParametersAware */ {

    private SalesforceClient salesforceClient;
    private JettyHttpClient jettyHttpClient;
    private CometDClient cometDClient;

    public SalesforceEmpComponent(CamelContext context, ConnectionConfig connectionConfig) {
        super(context);

        jettyHttpClient = new JettyHttpClient();
        salesforceClient = new SalesforceClient(connectionConfig, jettyHttpClient);
        cometDClient = new CometDClient(this);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        String[] remainingParts = remaining.split(":");
        if (remainingParts.length != 2) {
            throw new CamelException("Invalid uri. Expecting: sf-emp:[type]:[topic]");
        }
        SalesforceEmpEndpoint.Type endpointType = SalesforceEmpEndpoint.Type.ofValue(remainingParts[0]);
        String topic = remainingParts[1];
        Endpoint endpoint = new SalesforceEmpEndpoint(uri, this, endpointType, topic);
        setProperties(endpoint, parameters);
        return endpoint;
    }

    public JettyHttpClient getJettyHttpClient() {
        return jettyHttpClient;
    }

    public SalesforceClient getSalesforceClient() {
        return salesforceClient;
    }

    public CometDClient getCometDClient() {
        return cometDClient;
    }

    @Override
    protected void doStart() throws Exception {
        ServiceHelper.startService(jettyHttpClient);
        ServiceHelper.startService(salesforceClient);
        ServiceHelper.startService(cometDClient);
    }

    @Override
    protected void doStop() throws Exception {
        ServiceHelper.stopService(salesforceClient);
        ServiceHelper.stopService(jettyHttpClient);
        ServiceHelper.stopService(cometDClient);
    }

    /*
    @Override
    public boolean isUseGlobalSslContextParameters() {
        return false;
    }

    @Override
    public void setUseGlobalSslContextParameters(boolean b) {
    }
    */
}
