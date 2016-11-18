package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlEmptyQueryMessage extends PgsqlQueryResponseMessage {

    public static final byte TYPE = (byte) 'I';

    @Override
    public byte getType() {
        return TYPE;
    }
}
