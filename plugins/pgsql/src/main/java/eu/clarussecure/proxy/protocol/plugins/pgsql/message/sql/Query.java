package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

public interface Query {
    default void retain() {
    }

    default boolean release() {
        return true;
    }
}
