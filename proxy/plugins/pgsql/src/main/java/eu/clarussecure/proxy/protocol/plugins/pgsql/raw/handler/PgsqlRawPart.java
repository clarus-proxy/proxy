package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.FilterableMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

public interface PgsqlRawPart extends ByteBufHolder, FilterableMessage {

    default ByteBuf getBytes() {
        return content();
    }

    @Override
    boolean filter();

    void filter(boolean filter);
}
