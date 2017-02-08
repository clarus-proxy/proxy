package eu.clarussecure.proxy.protocol.plugins.http;

import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.http.message.HttpMessage;
import eu.clarussecure.proxy.protocol.plugins.http.message.parser.HttpMessageParser;
import eu.clarussecure.proxy.protocol.plugins.http.message.writer.HttpMessageWriter;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import io.netty.util.AttributeKey;

public interface HttpConstants extends TCPConstants {

	AttributeKey<Map<Class<? extends HttpMessage>, HttpMessageParser<? extends HttpMessage>>> MSG_PARSERS_KEY = AttributeKey
			.<Map<Class<? extends HttpMessage>, HttpMessageParser<? extends HttpMessage>>>newInstance(
					"MSG_PARSERS_KEY");

	AttributeKey<Map<Class<? extends HttpMessage>, HttpMessageWriter<? extends HttpMessage>>> MSG_WRITERS_KEY = AttributeKey
			.<Map<Class<? extends HttpMessage>, HttpMessageWriter<? extends HttpMessage>>>newInstance(
					"MSG_WRITERS_KEY");
}
