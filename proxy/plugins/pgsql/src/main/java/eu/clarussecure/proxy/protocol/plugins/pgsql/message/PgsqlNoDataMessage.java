package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlNoDataMessage extends PgsqlQueryResponseMessage {

    public static final byte TYPE = (byte) 'n';

    @Override
    public byte getType() {
        return TYPE;
    }
}
