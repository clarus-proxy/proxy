package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;

public abstract class PgsqlQueryMessage extends PgsqlQueryRequestMessage {

    protected CString query;

    public PgsqlQueryMessage(CString query) {
        this.query = Objects.requireNonNull(query, "query must not be null");
    }

    public CString getQuery() {
        return query;
    }

    public void setQuery(CString query) {
        this.query = Objects.requireNonNull(query, "query must not be null");
    }
}
