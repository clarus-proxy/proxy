package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import io.netty.util.internal.StringUtil;

public class PasswordStep implements ExtendedQuery {

    public PasswordStep() {
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this);
    }
}
