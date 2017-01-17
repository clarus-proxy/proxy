package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlParseCompleteMessage extends PgsqlQueryResponseMessage {

    public static final byte TYPE = (byte) '1';

    @Override
    public byte getType() {
        return TYPE;
    }
}
