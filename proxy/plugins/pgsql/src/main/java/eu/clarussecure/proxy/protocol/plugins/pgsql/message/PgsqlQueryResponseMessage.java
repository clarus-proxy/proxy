package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public interface PgsqlQueryResponseMessage<T> extends PgsqlMessage {
    T getDetails();
}
