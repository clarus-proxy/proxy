package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

public class PgsqlAuthenticationCleartextPasswordMessage implements PgsqlAuthenticationMessage{

    public static final byte TYPE = (byte) 'R';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public int getHeaderSize() {
        return HEADER_SIZE;
    }

}
