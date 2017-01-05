package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawHeader;

public interface MutablePgsqlRawMessage extends PgsqlRawHeader, PgsqlRawContent {

    void setMissing(int missing);

    int getMissing();

    default boolean isComplete() {
        return getMissing() == 0;
    }
}
