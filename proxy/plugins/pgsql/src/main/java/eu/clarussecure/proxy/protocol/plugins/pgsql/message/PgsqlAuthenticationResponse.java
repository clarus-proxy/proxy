package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import io.netty.buffer.ByteBuf;

public class PgsqlAuthenticationResponse implements PgsqlAuthenticationMessage {

    public static final byte TYPE = (byte) 'R';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    private int authenticationType;
    private ByteBuf authenticationParameters;

    public PgsqlAuthenticationResponse(int authenticationType, ByteBuf authenticationParameters) {
        this.authenticationType = authenticationType;
        this.setAuthenticationParameters(authenticationParameters);
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    public int getAuthenticationType() {
        return authenticationType;
    }

    public void setAuthenticationType(int authenticationType) {
        this.authenticationType = authenticationType;
    }

    public ByteBuf getAuthenticationParameters() {
        return authenticationParameters;
    }

    public void setAuthenticationParameters(ByteBuf authenticationParameters) {
        this.authenticationParameters = authenticationParameters;
    }

}
