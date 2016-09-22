package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import eu.clarussecure.proxy.spi.CString;

public abstract class PgsqlQueryMessage implements PgsqlMessage {

    public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES;

    protected CString query;

    public PgsqlQueryMessage(CString query) {
        this.query = query;
    }

    public CString getQuery() {
        return query;
    }

    public void setQuery(CString query) {
        this.query = query;
    }

    @Override
    public int getHeaderSize() {
        return HEADER_SIZE;
    }
}
