package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;

public class BindStep implements ExtendedQuery {
    private CString name;
    private CString preparedStatement;
    private List<Short> parameterFormats;
    private List<ByteBuf> parameterValues;
    private List<Short> resultColumnFormats;

    public BindStep(CString portal, CString preparedStatement, List<Short> parameterFormats,
            List<ByteBuf> parameterValues, List<Short> resultColumnFormats) {
        this.name = Objects.requireNonNull(portal, "portal must not be null");
        this.preparedStatement = Objects.requireNonNull(preparedStatement, "preparedStatement must not be null");
        this.parameterFormats = Objects.requireNonNull(parameterFormats, "parameterFormats must not be null");
        this.parameterValues = Objects.requireNonNull(parameterValues, "parameterValues must not be null");
        this.resultColumnFormats = Objects.requireNonNull(resultColumnFormats, "resultColumnFormats must not be null");
    }

    public CString getName() {
        return name;
    }

    public void setName(CString name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
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

    public List<ByteBuf> getParameterValues() {
        return parameterValues;
    }

    public void setParameterValues(List<ByteBuf> parameterValues) {
        this.parameterValues = Objects.requireNonNull(parameterValues, "parameterValues must not be null");
    }

    public List<Short> getResultColumnFormats() {
        return resultColumnFormats;
    }

    public void setResultColumnFormats(List<Short> resultColumnFormats) {
        this.resultColumnFormats = Objects.requireNonNull(resultColumnFormats, "resultColumnFormats must not be null");
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [name=").append(name);
        builder.append(", preparedStatement=").append(preparedStatement);
        builder.append(", parameterFormats=").append(parameterFormats);
        builder.append(", parameterValues=").append(parameterValues);
        builder.append(", resultColumnFormats=").append(resultColumnFormats);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public void retain() {
        if (name.isBuffered()) {
            name.retain();
        }
        if (preparedStatement.isBuffered()) {
            preparedStatement.retain();
        }
        parameterValues.forEach(b -> b.retain());
    }

    @Override
    public boolean release() {
        boolean portalDeallocated = true;
        if (name.isBuffered()) {
            portalDeallocated = name.release();
        }
        boolean preparedStatementDeallocated = true;
        if (preparedStatement.isBuffered()) {
            preparedStatementDeallocated = preparedStatement.release();
        }
        Boolean parameterValuesDeallocated = parameterValues.stream().reduce(true, (a, b) -> a & b.release(),
                (a, b) -> a & b);
        return portalDeallocated && preparedStatementDeallocated && parameterValuesDeallocated;
    }
}
