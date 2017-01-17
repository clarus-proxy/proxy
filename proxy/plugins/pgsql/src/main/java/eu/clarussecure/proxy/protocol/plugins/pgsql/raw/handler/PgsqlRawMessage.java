package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;

public interface PgsqlRawMessage extends PgsqlRawHeader, PgsqlRawContent {

    @Override
    ByteBuf getContent();

}
