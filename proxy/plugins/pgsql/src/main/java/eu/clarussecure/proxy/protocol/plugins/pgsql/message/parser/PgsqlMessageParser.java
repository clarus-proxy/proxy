package eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlMessage;
import io.netty.buffer.ByteBuf;

public interface PgsqlMessageParser<T extends PgsqlMessage> {

    T parse(ByteBuf content) throws IOException;
}
