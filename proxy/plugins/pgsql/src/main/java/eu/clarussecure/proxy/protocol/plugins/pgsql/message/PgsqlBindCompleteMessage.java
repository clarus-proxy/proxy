package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlBindCompleteMessage extends PgsqlQueryResponseMessage {

    public static final byte TYPE = (byte) '2';

    @Override
    public byte getType() {
        return TYPE;
    }
}
