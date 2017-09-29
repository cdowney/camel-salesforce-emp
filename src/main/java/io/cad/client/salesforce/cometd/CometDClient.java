package io.cad.client.salesforce.cometd;

import io.cad.SalesforceEmpComponent;
import io.cad.SalesforceEmpConsumer;
import io.cad.client.salesforce.auth.SalesforceClient;
import org.apache.camel.CamelException;
import org.apache.camel.support.ServiceSupport;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.cometd.bayeux.Channel.META_CONNECT;
import static org.cometd.bayeux.Channel.META_DISCONNECT;
import static org.cometd.bayeux.Channel.META_HANDSHAKE;

public class CometDClient extends ServiceSupport {

    private static final Logger LOG = LoggerFactory.getLogger(CometDClient.class);

    private static final Long CONNECT_TIMEOUT = 30L;
    private static final TimeUnit TIMEOUT_TIME_UNIT = TimeUnit.SECONDS;
    private static final Integer MAX_BUFFER_SIZE = 1048576;
    private static final Integer MAX_NETWORK_DELAY = 35000;
    private static final CometDReplayExtension REPLAY_EXTENSION = new CometDReplayExtension();

    private final SalesforceEmpComponent salesforceEmpComponent;
    private final ConcurrentHashMap<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    private BayeuxClient bayeuxClient;

    private ClientSessionChannel.MessageListener handshakeListener = (channel, message) -> {
        LOG.debug("[CHANNEL:META_HANDSHAKE]: {}", message);
        if (!message.isSuccessful()) {
            restartClient();
        }
    };

    private ClientSessionChannel.MessageListener connectListener = (channel, message) -> {
        LOG.debug("[CHANNEL:META_CONNECT]: {}", message);
        if (message.isSuccessful()) {
            resubscribe();
        }
    };

    private ClientSessionChannel.MessageListener disconnectListener = (channel, message) -> {
        LOG.debug("[CHANNEL:META_DISCONNECT]: {}", message);
        restartClient();
    };

    public CometDClient(SalesforceEmpComponent salesforceEmpComponent) {
        this.salesforceEmpComponent = salesforceEmpComponent;
    }

    @Override
    protected void doStart() throws Exception {
        subscriptions.clear();

        bayeuxClient = createClient();

        bayeuxClient.getChannel(META_HANDSHAKE).addListener(handshakeListener);
        bayeuxClient.getChannel(META_CONNECT).addListener(connectListener);
        bayeuxClient.getChannel(META_DISCONNECT).addListener(disconnectListener);

        bayeuxClient.handshake();
        if (!bayeuxClient.waitFor(MILLISECONDS.convert(CONNECT_TIMEOUT, TIMEOUT_TIME_UNIT), BayeuxClient.State.CONNECTED)) {
            throw new CamelException("Timeout connecting to " + salesforceEmpComponent.getSalesforceClient().getCometdEndpoint());
        }
    }

    @Override
    protected void doStop() throws Exception {
        bayeuxClient.getChannel(META_HANDSHAKE).removeListener(handshakeListener);
        bayeuxClient.getChannel(META_CONNECT).removeListener(connectListener);
        bayeuxClient.getChannel(META_DISCONNECT).removeListener(disconnectListener);

        bayeuxClient.disconnect();

        bayeuxClient = null;
    }

    private BayeuxClient createClient() throws Exception {
        HttpClient httpClient = salesforceEmpComponent.getJettyHttpClient().getHttpClient();
        SalesforceClient salesforceClient = salesforceEmpComponent.getSalesforceClient();

        Map<String, Object> longPollingOptions = new HashMap<String, Object>() {
            {
                put(ClientTransport.MAX_NETWORK_DELAY_OPTION, MAX_NETWORK_DELAY);
                //                put(ClientTransport.MAX_MESSAGE_SIZE_OPTION, MAX_BUFFER_SIZE);
            }
        };

        LongPollingTransport transport = new LongPollingTransport(longPollingOptions, httpClient) {
            @Override
            protected void customize(Request request) {
                super.customize(request);
                request.getHeaders().put(HttpHeader.AUTHORIZATION, "OAuth " + salesforceClient.getBearerToken());
            }
        };

        BayeuxClient client = new BayeuxClient(salesforceClient.getCometdEndpoint().toExternalForm(), transport);

        // added eagerly to check for support during handshake
        client.addExtension(REPLAY_EXTENSION);

        return client;
    }

    private synchronized void restartClient() {
        try {
            doStop();
            doStart();
        } catch (Exception exception) {
            LOG.error("Unable to restart client: {}", exception);
        }
    }

    private synchronized void resubscribe() {
        LOG.debug("Refreshing subscriptions to channels on reconnect");

        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            // TODO: unsubscribe?
            addSubscription(entry.getValue());
        }
    }

    public void addSubscription(Subscription subscription) {
        if (bayeuxClient.isDisconnected()) {
            throw new IllegalStateException(
                    String.format("Connector[%s] has not been started", salesforceEmpComponent.getSalesforceClient().getCometdEndpoint()));
        }

        LOG.info("Subscribing to channel: {}", subscription.getChannelName());
        REPLAY_EXTENSION.addChannelReplayId(subscription.getTopic(), subscription.getReplayFrom());

        ClientSessionChannel channel = bayeuxClient.getChannel(subscription.getChannelName());
        SalesforceEmpConsumer consumer = subscription.getConsumer();

        channel.subscribe(
                // On message listener
                (c, message) -> consumer.processMessage(message.getDataAsMap()),
                // On subscription listener
                (c, message) -> {
                    if (message.isSuccessful()) {
                        LOG.info("Successfully Subscribed to channel: {} {}", subscription.getChannelName(), c.getChannelId());
                        subscriptions.put(subscription.getName(), subscription);
                    } else {
                        LOG.error("Unable to subscribe to subscription {} : {}", subscription, message);
                    }
                });
    }

    public void removeSubscription(String name) {
        if (bayeuxClient.isDisconnected()) {
            throw new IllegalStateException(
                    String.format("Connector[%s] has not been started", salesforceEmpComponent.getSalesforceClient().getCometdEndpoint()));
        }

        // TODO: remove from REPLAY_EXTENSIONS?

        Subscription subscription = subscriptions.get(name);

        if (subscription == null) {
            LOG.warn("Subscription with name {} does not exist", name);
        } else {
            bayeuxClient.getChannel(subscription.getChannelName()).unsubscribe();
        }
    }

}
