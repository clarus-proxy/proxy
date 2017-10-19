package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;

public interface PgsqlRawContent extends PgsqlRawPart {

    default ByteBuf getContent() {
        return content();
    }

}
