package io.cad.consumer;

import io.cad.SalesforceEmpConsumer;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/*
Example Platform Event message

{
  "data": {
    "schema": "lGQFApBgdfD-mL0kJ55-rQ",
    "payload": {
      "CreatedById": "0051a000001w6xl",
      "operation__c": "delete",
      "CreatedDate": "2017-09-12T18:40:06Z",
      "name__c": "RegionServiceTestRegion",
      "salesforce_sales_territory_region_id__c": "a0b61000001FghwDAC",
      "regional_sales_director_user_id__c": null
    },
    "event": {
      "replayId": 3
    }
  },
  "channel": "/event/Sales_Territory_Regions__e"
}
 */

public class PlatformEventConsumer implements BiConsumer<SalesforceEmpConsumer, Map<String, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(PlatformEventConsumer.class);

    private static final String EVENT_KEY = "event";
    private static final String EVENT_REPLAY_ID_KEY = "replayId";
    private static final String SCHEMA_KEY = "schema";
    private static final String PAYLOAD_KEY = "payload";

    @Override
    public void accept(SalesforceEmpConsumer salesforceEmpConsumer, Map<String, Object> data) {
        LOG.debug("Received message: {}", data);

        final Exchange exchange = salesforceEmpConsumer.getEndpoint().createExchange();
        Message in = exchange.getIn();
        setHeaders(in, data);

        String schema = data.get(SCHEMA_KEY).toString();

        @SuppressWarnings("unchecked")
        Long replayId = Long.valueOf(((Map<String, Object>) data.get(EVENT_KEY)).get(EVENT_REPLAY_ID_KEY).toString());

        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) data.get(PAYLOAD_KEY);

        in.setHeader("CamelSalesforceSchema", schema);
        if (replayId != null) {
            in.setHeader("CamelSalesforceReplayId", replayId);
        }

        String payloadString = payload.toString();
        LOG.debug("Received SObject: {}", payloadString);

        in.setBody(payloadString);

        try {
            salesforceEmpConsumer.getAsyncProcessor().process(exchange, new AsyncCallback() {
                public void done(boolean doneSync) {
                    // noop
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Done processing event: {} {}", data.toString(),
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
        // TODO: headers.put("CamelSalesforceTopicName", topicName);

        // set message properties as headers
        // TODO: headers.put("CamelSalesforceChannel", message.getChannel());
        // TODO: headers.put("CamelSalesforceClientId", message.getClientId());

        in.setHeaders(headers);
    }
}
