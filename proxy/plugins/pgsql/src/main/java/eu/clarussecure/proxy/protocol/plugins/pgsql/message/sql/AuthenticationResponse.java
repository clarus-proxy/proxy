package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import io.netty.buffer.ByteBuf;

public class AuthenticationResponse {
    private int type;
    private ByteBuf parameters;

    public AuthenticationResponse(int type, ByteBuf parameters) {
        this.type = type;
        this.parameters = parameters;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public ByteBuf getParameters() {
        return parameters;
    }

    public void setParameters(ByteBuf parameters) {
        this.parameters = parameters;
    }
}
