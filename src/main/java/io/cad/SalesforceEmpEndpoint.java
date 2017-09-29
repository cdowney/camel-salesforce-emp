package io.cad;

import lombok.Getter;
import lombok.Setter;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriPath;

/**
 * Represents a SalesforceEmp endpoint.
 */
@UriEndpoint(scheme = "sf-emp", title = "SalesforceEmp", syntax = "sf-emp:name", consumerClass = SalesforceEmpConsumer.class, label = "SalesforceEmp")
public class SalesforceEmpEndpoint extends DefaultEndpoint {
    public enum Type {
        PUSH_TOPIC("pushTopic"),
        PLATFORM_EVENT("platformEvent");

        private String value;

        Type(String value) {
            this.value = value;
        }

        public static Type ofValue(String value) {
            for (Type type : Type.values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            return null;
        }
    }

    @UriPath(label = "name", description = "Endpoint name") @Metadata(required = "true")
    @Getter
    @Setter
    private String name;

    @UriPath(label = "type", description = "pushTopic or platformEvent") @Metadata(required = "true")
    @Getter
    @Setter
    private Type type;

    @UriPath(label = "topic", description = "EMP topic") @Metadata(required = "true")
    @Getter
    @Setter
    private String topic;

    private SalesforceEmpComponent component;

    public Type getType() {
        return type;
    }

    public String getTopic() {
        return topic;
    }

    public String getName() {
        return name;
    }

    public SalesforceEmpComponent getComponent() {
        return component;
    }

    public SalesforceEmpEndpoint(String uri, SalesforceEmpComponent component, Type type, String topic) {
        super(uri, component);
        this.type = type;
        this.topic = topic;
        this.name = topic; // TODO: set name?
        this.component = component;
    }

    @Override
    public Producer createProducer() throws Exception {
        return null;
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        return new SalesforceEmpConsumer(this, processor);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
