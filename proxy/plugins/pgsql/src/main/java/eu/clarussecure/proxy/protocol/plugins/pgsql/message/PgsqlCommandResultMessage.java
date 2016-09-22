package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import eu.clarussecure.proxy.spi.CString;

public interface PgsqlCommandResultMessage extends PgsqlQueryResponseMessage {
    boolean isSuccess();
    CString getDetails();
    void setDetails(CString details);
}
