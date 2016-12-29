package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

public interface PgsqlRawHeader extends PgsqlRawPart {

    byte getType();

    default int getHeaderSize() {
        return (getType() == 0 ? 0 : Byte.BYTES) + Integer.BYTES;
    }

    int getLength();

    default int getTotalLength() {
        return (getType() == 0 ? 0 : Byte.BYTES) + getLength();
    }
}
