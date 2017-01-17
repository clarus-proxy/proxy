package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class ParseStep implements ExtendedQuery, SQLStatement {
    private CString name;
    private CString sql;
    private boolean metadata;
    private List<CString> columns;
    private List<Long> parameterTypes;

    public ParseStep(CString name, CString sql, boolean metadata, List<CString> columns, List<Long> parameterTypes) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
        this.metadata = metadata;
        this.columns = columns;
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

    public boolean isMetadata() {
        return metadata;
    }

    public void setMetadata(boolean metadata) {
        this.metadata = metadata;
    }

    public List<CString> getColumns() {
        return columns;
    }

    public void setColumns(List<CString> columns) {
        this.columns = columns;
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
        builder.append(", metadata=").append(metadata);
        builder.append(", columns=").append(columns);
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
        if (columns != null) {
            for (CString column : columns) {
                if (column.isBuffered()) {
                    column.retain();
                }
            }
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
        boolean columnsDeallocated = true;
        if (columns != null) {
            for (CString column : columns) {
                if (column.isBuffered()) {
                    columnsDeallocated &= column.release();
                }
            }
        }
        return nameDeallocated && sqlDeallocated && columnsDeallocated;
    }
}
