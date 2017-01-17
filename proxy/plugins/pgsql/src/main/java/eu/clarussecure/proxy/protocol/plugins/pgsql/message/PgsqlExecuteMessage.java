package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlExecuteMessage extends PgsqlQueryRequestMessage {

    public static final byte TYPE = (byte) 'E';

    private CString portal;
    private int maxRows;

    public PgsqlExecuteMessage(CString portal, int maxRows) {
        this.portal = Objects.requireNonNull(portal, "portal must not be null");
        this.maxRows = maxRows;
    }

    public CString getPortal() {
        return portal;
    }

    public void setPortal(CString portal) {
        this.portal = Objects.requireNonNull(portal, "portal must not be null");
    }

    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
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
        builder.append(", mawRows=").append(maxRows);
        builder.append("]");
        return builder.toString();
    }
}
