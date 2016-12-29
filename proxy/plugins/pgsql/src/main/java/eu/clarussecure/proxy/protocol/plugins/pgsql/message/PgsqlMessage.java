package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public interface PgsqlMessage {

    byte getType();

    int getHeaderSize();
}
