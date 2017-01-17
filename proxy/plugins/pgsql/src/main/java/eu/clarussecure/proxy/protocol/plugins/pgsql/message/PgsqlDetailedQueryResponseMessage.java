package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public abstract class PgsqlDetailedQueryResponseMessage<T> extends PgsqlQueryResponseMessage {
    abstract T getDetails();
}
