package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLRequestMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLResponseMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;

public interface PgsqlRawHeader extends PgsqlRawPart {

    byte getType();

    default int getHeaderSize() {
        switch (getType()) {
        case PgsqlSSLRequestMessage.TYPE:
        case PgsqlStartupMessage.TYPE:
            return Integer.BYTES;
        case PgsqlSSLResponseMessage.TYPE:
            return 0;
        default:
            return Byte.BYTES + Integer.BYTES;
        }
    }

    int getLength();

    default int getTotalLength() {
        switch (getType()) {
        case PgsqlSSLRequestMessage.TYPE:
        case PgsqlStartupMessage.TYPE:
            return Integer.BYTES + getLength();
        case PgsqlSSLResponseMessage.TYPE:
            return getLength();
        default:
            return Byte.BYTES + getLength();
        }
    }
}
