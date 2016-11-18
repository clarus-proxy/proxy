package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class ParseStep implements ExtendedQuery, SQLStatement {
    private CString name;
    private CString sql;
    private List<Long> parameterTypes;

    public ParseStep(CString name, CString sql, List<Long> parameterTypes) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
        this.parameterTypes = Objects.requireNonNull(parameterTypes, "parameterTypes must not be null");
    }

    public CString getName() {
        return name;
    }

    public void setName(CString name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    @Override
    public CString getSQL() {
        return sql;
    }

    public void setSQL(CString sql) {
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
    }

    public List<Long> getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(List<Long> parameterTypes) {
        this.parameterTypes = Objects.requireNonNull(parameterTypes, "parameterTypes must not be null");
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [name=").append(name);
        builder.append(", SQL=").append(sql);
        builder.append(", parameterTypes=").append(parameterTypes);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public void retain() {
        if (name.isBuffered()) {
            name.retain();
        }
        if (sql.isBuffered()) {
            sql.retain();
        }
    }

    @Override
    public boolean release() {
        boolean nameDeallocated = true;
        if (name.isBuffered()) {
            nameDeallocated = name.release();
        }
        boolean sqlDeallocated = true;
        if (sql.isBuffered()) {
            sqlDeallocated = sql.release();
        }
        return nameDeallocated && sqlDeallocated;
    }
}
