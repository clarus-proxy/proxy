package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import io.netty.util.internal.StringUtil;

public class FlushStep implements ExtendedQuery {

    public FlushStep() {
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this);
    }

}
