package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlCloseCompleteMessage extends PgsqlQueryResponseMessage {

    public static final byte TYPE = (byte) '3';

    @Override
    public byte getType() {
        return TYPE;
    }
}
