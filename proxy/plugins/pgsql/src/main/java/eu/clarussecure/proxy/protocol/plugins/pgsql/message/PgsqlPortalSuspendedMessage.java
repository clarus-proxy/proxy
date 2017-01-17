package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlPortalSuspendedMessage extends PgsqlQueryResponseMessage {

    public static final byte TYPE = (byte) 's';

    @Override
    public byte getType() {
        return TYPE;
    }
}
