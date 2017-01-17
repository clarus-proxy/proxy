package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlSyncMessage extends PgsqlQueryRequestMessage {

    public static final byte TYPE = (byte) 'S';

    @Override
    public byte getType() {
        return TYPE;
    }
}
