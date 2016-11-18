package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlParameterStatusMessage extends PgsqlDetailedQueryResponseMessage<List<CString>> {

    public static final byte TYPE = (byte) 'S';

    private CString name;
    private CString value;

    public PgsqlParameterStatusMessage(CString name, CString value) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.value = Objects.requireNonNull(name, "value must not be null");
    }

    public CString getName() {
        return name;
    }

    public void setName(CString name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    public CString getValue() {
        return value;
    }

    public void setValue(CString value) {
        this.value = Objects.requireNonNull(value, "value must not be null");
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("name=").append(name);
        builder.append("value=").append(value);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public List<CString> getDetails() {
        return Stream.of(getName(), getValue()).collect(Collectors.toList());
    }
}
