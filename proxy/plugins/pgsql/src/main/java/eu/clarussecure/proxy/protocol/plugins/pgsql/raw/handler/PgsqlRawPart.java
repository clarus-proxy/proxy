package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

public interface PgsqlRawPart extends ByteBufHolder {

    default ByteBuf getBytes() {
        return content();
    }

}
