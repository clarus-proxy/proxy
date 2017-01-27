package eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder;

public interface FilterableMessage {

    default boolean filter() {
        return true;
    }
}
