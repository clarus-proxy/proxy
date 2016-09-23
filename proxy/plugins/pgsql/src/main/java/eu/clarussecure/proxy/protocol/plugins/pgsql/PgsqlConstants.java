package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.parser.PgsqlMessageParser;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.writer.PgsqlMessageWriter;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import io.netty.util.AttributeKey;

public interface PgsqlConstants extends TCPConstants {

    AttributeKey<Map<Class<? extends PgsqlMessage>, PgsqlMessageParser<? extends PgsqlMessage>>> MSG_PARSERS_KEY = AttributeKey.<Map<Class<? extends PgsqlMessage>, PgsqlMessageParser<? extends PgsqlMessage>>>newInstance("MSG_PARSERS_KEY");

    AttributeKey<Map<Class<? extends PgsqlMessage>, PgsqlMessageWriter<? extends PgsqlMessage>>> MSG_WRITERS_KEY = AttributeKey.<Map<Class<? extends PgsqlMessage>, PgsqlMessageWriter<? extends PgsqlMessage>>>newInstance("MSG_WRITERS_KEY");
}
