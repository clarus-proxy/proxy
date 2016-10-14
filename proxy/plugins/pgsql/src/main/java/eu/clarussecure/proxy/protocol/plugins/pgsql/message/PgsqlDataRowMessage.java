package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.List;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.internal.StringUtil;

public class PgsqlDataRowMessage implements PgsqlQueryResponseMessage<List<ByteBuf>> {

    public static final byte TYPE = (byte) 'D';
    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    private List<ByteBuf> values;

    public PgsqlDataRowMessage(List<ByteBuf> values) {
        this.values = Objects.requireNonNull(values, "values must not be null");
    }

    public List<ByteBuf> getValues() {
        return values;
    }

    public void setValues(List<ByteBuf> values) {
        this.values = Objects.requireNonNull(values, "values must not be null");
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("values=");
        int i = 0;
        for (ByteBuf value : values) {
            builder.append(ByteBufUtil.hexDump(value, 0, value.capacity()));
            if (++i < values.size()) {
                builder.append('\n');
            }
        }
        builder.append("]");
        return builder.toString();
    }

    @Override
    public List<ByteBuf> getDetails() {
        return getValues();
    }
}
