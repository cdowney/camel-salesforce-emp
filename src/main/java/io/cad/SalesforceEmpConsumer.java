package io.cad;

import io.cad.client.salesforce.cometd.Subscription;
import io.cad.consumer.PlatformEventConsumer;
import io.cad.consumer.PushTopicEventConsumer;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * The SalesforceEmp consumer.
 */
public class SalesforceEmpConsumer extends DefaultConsumer {

    private BiConsumer<SalesforceEmpConsumer, Map<String, Object>> messageProcessor;

    public SalesforceEmpConsumer(SalesforceEmpEndpoint endpoint, Processor processor) {
        super(endpoint, processor);

        if (endpoint.getType().equals(SalesforceEmpEndpoint.Type.PLATFORM_EVENT)) {
            messageProcessor = new PlatformEventConsumer();
        } else if (endpoint.getType().equals(SalesforceEmpEndpoint.Type.PUSH_TOPIC)) {
            messageProcessor = new PushTopicEventConsumer();
        }

        Subscription subscription = Subscription.builder()
                .consumer(this)
                .name(endpoint.getName())
                .replayFrom(-1L)
                .topic(endpoint.getTopic())
                .type(endpoint.getType())
                .build();

        endpoint.getComponent()
                .getCometDClient()
                .addSubscription(subscription);
    }

    public void processMessage(Map<String, Object> message) {
        messageProcessor.accept(this, message);
    }

    public void handleException(String message, Throwable throwable) {
        this.handleException(message, throwable);
    }

}
