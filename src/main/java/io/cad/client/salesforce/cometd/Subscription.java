package io.cad.client.salesforce.cometd;

import io.cad.SalesforceEmpConsumer;
import io.cad.SalesforceEmpEndpoint;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@Builder
@EqualsAndHashCode
@ToString
public class Subscription {

    private String name;
    private SalesforceEmpEndpoint.Type type;
    private String topic;
    private SalesforceEmpConsumer consumer;

    @Builder.Default
    private Long replayFrom = -1L;

    public String getChannelName() {
        switch (type) {
        case PUSH_TOPIC:
            return "/topic/" + topic;
        case PLATFORM_EVENT:
            return "/event/" + topic;
        }
        throw new IllegalStateException("Subscription type not defined");
    }
}