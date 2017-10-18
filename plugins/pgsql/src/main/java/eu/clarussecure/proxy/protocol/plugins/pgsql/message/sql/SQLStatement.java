package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import eu.clarussecure.proxy.spi.CString;

public interface SQLStatement extends Query {
    CString getSQL();
}
