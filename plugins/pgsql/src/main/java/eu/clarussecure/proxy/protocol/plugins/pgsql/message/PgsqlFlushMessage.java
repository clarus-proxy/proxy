package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlFlushMessage extends PgsqlQueryRequestMessage {

    public static final byte TYPE = (byte) 'H';

    @Override
    public byte getType() {
        return TYPE;
    }
}
