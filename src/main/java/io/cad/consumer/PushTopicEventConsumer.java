package io.cad.consumer;

import io.cad.SalesforceEmpConsumer;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class PushTopicEventConsumer implements BiConsumer<SalesforceEmpConsumer, Map<String, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(PushTopicEventConsumer.class);

    private static final String EVENT_PROPERTY = "event";
    private static final String TYPE_PROPERTY = "type";
    private static final String CREATED_DATE_PROPERTY = "createdDate";
    private static final String SOBJECT_PROPERTY = "sobject";
    private static final String REPLAY_ID_PROPERTY = "replayId";

    private String topicName = "topicName"; // TODO: is this needed?

    @Override
    public void accept(SalesforceEmpConsumer salesforceEmpConsumer, Map<String, Object> data) {
        final Exchange exchange = salesforceEmpConsumer.getEndpoint().createExchange();
        org.apache.camel.Message in = exchange.getIn();
        setHeaders(in, data);

        // get event data
        // TODO: do we need to add NPE checks for message/data.get***???
        //        Map<String, Object> data = message.getDataAsMap();

        @SuppressWarnings("unchecked") final Map<String, Object> event = (Map<String, Object>) data.get(EVENT_PROPERTY);
        final Object eventType = event.get(TYPE_PROPERTY);
        Object createdDate = event.get(CREATED_DATE_PROPERTY);
        Object replayId = event.get(REPLAY_ID_PROPERTY);

        /*
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Received event %s on channel %s created on %s",
                    eventType, channel.getChannelId(), createdDate));
        }
        */

        in.setHeader("CamelSalesforceEventType", eventType);
        in.setHeader("CamelSalesforceCreatedDate", createdDate);
        if (replayId != null) {
            in.setHeader("CamelSalesforceReplayId", replayId);
        }

        // get SObject
        @SuppressWarnings("unchecked") final Map<String, Object> sObject = (Map<String, Object>) data.get(SOBJECT_PROPERTY);
        /*
        try {

            final String sObjectString = objectMapper.writeValueAsString(sObject);
            */
        String sObjectString = sObject.toString();
        LOG.debug("Received SObject: {}", sObjectString);

            /*
            if (endpoint.getConfiguration().getRawPayload()) {
                // return sobject string as exchange body
                in.setBody(sObjectString);
            } else if (sObjectClass == null) {
                // return sobject map as exchange body
                in.setBody(sObject);
            } else {
                // create the expected SObject
                in.setBody(objectMapper.readValue(
                        new StringReader(sObjectString), sObjectClass));
            }
            */
        in.setBody(sObjectString);

            /*
        } catch (IOException e) {
            final String msg = String.format("Error parsing message [%s] from Topic %s: %s", data, topicName, e.getMessage());
            salesforceEmpConsumer.handleException(msg, new Exception(msg, e));
        }
        */

        try {
            salesforceEmpConsumer.getAsyncProcessor().process(exchange, new AsyncCallback() {
                public void done(boolean doneSync) {
                    // noop
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Done processing event: {} {}", eventType.toString(),
                                doneSync ? "synchronously" : "asynchronously");
                    }
                }
            });
        } catch (Exception e) {
            String msg = String.format("Error processing %s: %s", exchange, e);
            salesforceEmpConsumer.handleException(msg, new Exception(msg, e));
        } finally {
            Exception ex = exchange.getException();
            if (ex != null) {
                String msg = String.format("Unhandled exception: %s", ex.getMessage());
                salesforceEmpConsumer.handleException(msg, new Exception(msg, ex));
            }
        }
    }

    private void setHeaders(org.apache.camel.Message in, Map<String, Object> message) {
        Map<String, Object> headers = new HashMap<String, Object>();
        // set topic name
        headers.put("CamelSalesforceTopicName", topicName);

        // set message properties as headers
        // TODO: headers.put("CamelSalesforceChannel", message.getChannel());
        // TODO: headers.put("CamelSalesforceClientId", message.getClientId());

        in.setHeaders(headers);
    }
}
