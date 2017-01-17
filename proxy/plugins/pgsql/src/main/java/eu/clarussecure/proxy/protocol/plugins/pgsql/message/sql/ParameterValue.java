package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import io.netty.buffer.ByteBuf;

public class ParameterValue {
    private long type;
    private short format;
    private ByteBuf value;

    public ParameterValue(ByteBuf value) {
        this(0L, (short) 0, value);
    }

    public ParameterValue(long type, ByteBuf value) {
        this(type, (short) 0, value);
    }

    public ParameterValue(short format, ByteBuf value) {
        this(0, format, value);
    }

    public ParameterValue(long type, short format, ByteBuf value) {
        this.type = type;
        this.format = format;
        this.value = value;
    }

    public long getType() {
        return type;
    }

    public void setType(long type) {
        this.type = type;
    }

    public short getFormat() {
        return format;
    }

    public void setFormat(short format) {
        this.format = format;
    }

    public ByteBuf getValue() {
        return value;
    }

    public void setValue(ByteBuf value) {
        this.value = value;
    }

}
