package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class ExecuteStep implements ExtendedQuery {

    private CString portal;
    private int maxRows;

    public ExecuteStep(CString portal, int maxRows) {
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
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [portal=").append(portal);
        builder.append(", mawRows=").append(maxRows);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public void retain() {
        if (portal.isBuffered()) {
            portal.retain();
        }
    }

    @Override
    public boolean release() {
        if (portal.isBuffered()) {
            return portal.release();
        }
        return true;
    }
}
