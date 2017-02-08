package eu.clarussecure.proxy.protocol.plugins.http.message.parser;

import java.io.IOException;

import eu.clarussecure.proxy.protocol.plugins.http.message.HttpMessage;
import io.netty.buffer.ByteBuf;

public interface HttpMessageParser<T extends HttpMessage> {

	T parse(ByteBuf content) throws IOException;
}
