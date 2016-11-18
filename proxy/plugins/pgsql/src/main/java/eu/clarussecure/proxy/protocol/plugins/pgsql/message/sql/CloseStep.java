package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class CloseStep implements ExtendedQuery {

    private byte code;
    private CString name;

    public CloseStep(byte code, CString name) {
        this.code = code;
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    public byte getCode() {
        return code;
    }

    public void setCode(byte code) {
        this.code = code;
    }

    public CString getName() {
        return name;
    }

    public void setName(CString name) {
        this.name = name;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [code=").append(code);
        builder.append(", name=").append(name);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public void retain() {
        if (name.isBuffered()) {
            name.retain();
        }
    }

    @Override
    public boolean release() {
        if (name.isBuffered()) {
            return name.release();
        }
        return true;
    }
}
