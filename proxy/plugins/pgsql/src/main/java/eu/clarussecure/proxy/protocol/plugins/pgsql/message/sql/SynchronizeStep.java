package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import io.netty.util.internal.StringUtil;

public class SynchronizeStep implements ExtendedQuery {

    public SynchronizeStep() {
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this);
    }

}
