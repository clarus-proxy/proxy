package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class SimpleSQLStatement implements SimpleQuery, SQLStatement {
    private CString sql;

    public SimpleSQLStatement(CString sql) {
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
    }

    @Override
    public CString getSQL() {
        return sql;
    }

    public void setSQL(CString sql) {
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("SQL=").append(sql);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public void retain() {
        if (sql.isBuffered()) {
            sql.retain();
        }
    }

    @Override
    public boolean release() {
        if (sql.isBuffered()) {
            return sql.release();
        }
        return true;
    }
}
