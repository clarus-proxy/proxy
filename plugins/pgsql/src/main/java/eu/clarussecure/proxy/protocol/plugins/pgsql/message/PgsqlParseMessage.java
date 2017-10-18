package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlParseMessage extends PgsqlQueryMessage {

    public static final byte TYPE = (byte) 'P';

    private CString preparedStatement;
    private List<Long> parameterTypes;

    public PgsqlParseMessage(CString preparedStatement, CString query, List<Long> parameterTypes) {
        super(query);
        this.preparedStatement = Objects.requireNonNull(preparedStatement, "preparedStatement must not be null");
        this.parameterTypes = Objects.requireNonNull(parameterTypes, "parameterTypes must not be null");
    }

    public CString getPreparedStatement() {
        return preparedStatement;
    }

    public void setPreparedStatement(CString preparedStatement) {
        this.preparedStatement = Objects.requireNonNull(preparedStatement, "preparedStatement must not be null");
    }

    public List<Long> getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(List<Long> parameterTypes) {
        this.parameterTypes = Objects.requireNonNull(parameterTypes, "parameterTypes must not be null");
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("preparedStatement=").append(preparedStatement);
        builder.append(", query=").append(query);
        builder.append(", parameterTypes=").append(parameterTypes);
        builder.append("]");
        return builder.toString();
    }
}
