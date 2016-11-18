package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;

public class PgsqlBindMessage extends PgsqlQueryRequestMessage {

    public static final byte TYPE = (byte) 'B';

    private CString portal;
    private CString preparedStatement;
    private List<Short> parameterFormats;
    private List<ByteBuf> parameterValues;
    private List<Short> resultColumnFormats;

    public PgsqlBindMessage(CString portal, CString preparedStatement, List<Short> parameterFormats, List<ByteBuf> parameterValues, List<Short> resultColumnFormats) {
        this.portal = Objects.requireNonNull(portal, "portal must not be null");
        this.preparedStatement = Objects.requireNonNull(preparedStatement, "preparedStatement must not be null");
        this.parameterFormats = Objects.requireNonNull(parameterFormats, "parameterFormats must not be null");
        this.parameterValues = Objects.requireNonNull(parameterValues, "parameterValues must not be null");
        this.resultColumnFormats = Objects.requireNonNull(resultColumnFormats, "resultColumnFormats must not be null");
    }

    public CString getPortal() {
        return portal;
    }

    public void setPortal(CString portal) {
        this.portal = Objects.requireNonNull(portal, "portal must not be null");
    }

    public CString getPreparedStatement() {
        return preparedStatement;
    }

    public void setPreparedStatement(CString preparedStatement) {
        this.preparedStatement = Objects.requireNonNull(preparedStatement, "preparedStatement must not be null");
    }

    public List<Short> getParameterFormats() {
        return parameterFormats;
    }

    public void setParameterFormats(List<Short> parameterFormats) {
        this.parameterFormats = Objects.requireNonNull(parameterFormats, "parameterFormats must not be null");
    }

    public short getParameterFormat(int index) {
        switch (parameterFormats.size()) {
        case 0:         // Text format
            return 0;
        case 1:         // Same format for all parameters
            return parameterFormats.get(0);
        default:        // Format by parameter
            return parameterFormats.get(index);
        }
    }

    public boolean isParameterTextFormat(int index) {
        return getParameterFormat(index) == 0;
    }

    public boolean isParameterBinaryFormat(int index) {
        return getParameterFormat(index) == 1;
    }

    public List<ByteBuf> getParameterValues() {
        return parameterValues;
    }

    public void setParameterValues(List<ByteBuf> parameterValues) {
        this.parameterValues = Objects.requireNonNull(parameterValues, "parameterValues must not be null");
    }

    public ByteBuf getParameterValue(int index) {
        return parameterValues.get(index);
    }

    public CString getParameterValueAsText(int index) {
        if (!isParameterTextFormat(index)) {
            throw new IllegalArgumentException(String.format("parameter value %n is binary", index));
        }
        ByteBuf value = getParameterValue(index);
        return CString.valueOf(value, value.capacity());
    }

    public List<Short> getResultColumnFormats() {
        return resultColumnFormats;
    }

    public void setResultColumnFormats(List<Short> resultColumnFormats) {
        this.resultColumnFormats = Objects.requireNonNull(resultColumnFormats, "resultColumnFormats must not be null");
    }

    public short getResultColumnFormat(int index) {
        switch (resultColumnFormats.size()) {
        case 0:         // Text format
            return 0;
        case 1:         // Same format for all result columns
            return resultColumnFormats.get(0);
        default:        // Format by result column
            return resultColumnFormats.get(index);
        }
    }

    public boolean isResultColumnTextFormat(int index) {
        return getResultColumnFormat(index) == 0;
    }

    public boolean isResultColumnBinaryFormat(int index) {
        return getResultColumnFormat(index) == 1;
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("portal=").append(portal);
        builder.append(", preparedStatement=").append(preparedStatement);
        builder.append(", parameterFormats=").append(parameterFormats);
        builder.append(", parameterValues=").append(parameterValues);
        builder.append(", resultColumnFormats=").append(resultColumnFormats);
        builder.append("]");
        return builder.toString();
    }
}
