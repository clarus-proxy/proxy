package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlCloseMessage extends PgsqlQueryRequestMessage {

    public static final byte TYPE = (byte) 'C';

    private byte code;
    private CString name;

    public PgsqlCloseMessage(byte code, CString name) {
        this.code = code;
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    public byte getCode() {
        return code;
    }

    public void setCode(byte code) {
        this.code = code;
    }

    public boolean isPreparedStatement() {
        return code == 'S';
    }

    public boolean isPortal() {
        return code == 'P';
    }

    public CString getName() {
        return name;
    }

    public void setName(CString name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("code=").append((char) code);
        builder.append(", name=").append(name);
        builder.append("]");
        return builder.toString();
    }
}
