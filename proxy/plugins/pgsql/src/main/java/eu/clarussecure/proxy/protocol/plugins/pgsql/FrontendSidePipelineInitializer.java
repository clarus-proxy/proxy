package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.AuthenticationRequestHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlBindMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCancelRequestMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCloseMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDescribeMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlExecuteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlFlushMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParseMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSSLRequestMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSimpleQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSyncMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.QueryRequestHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.SessionInitializationRequestHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartAccumulator;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartAggregator;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartCodec;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder.PgsqlRequestForwarder;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class FrontendSidePipelineInitializer extends ChannelInitializer<Channel> {

    private static boolean MESSAGE_PROCESSING_ACTIVATED;
    private static boolean QUERY_PROCESSING_ACTIVATED;
    static {
        String messageProcessing = System.getProperty("pgsql.message.processing", "true");
        MESSAGE_PROCESSING_ACTIVATED = Boolean.TRUE.toString().equalsIgnoreCase(messageProcessing)
                || "1".equalsIgnoreCase(messageProcessing) || "yes".equalsIgnoreCase(messageProcessing)
                || "on".equalsIgnoreCase(messageProcessing);
        String queryProcessing = System.getProperty("pgsql.query.processing", "true");
        QUERY_PROCESSING_ACTIVATED = Boolean.TRUE.toString().equalsIgnoreCase(queryProcessing)
                || "1".equalsIgnoreCase(queryProcessing) || "yes".equalsIgnoreCase(queryProcessing)
                || "on".equalsIgnoreCase(queryProcessing);
    }

    private EventExecutorGroup parserGroup = null;

    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(PgsqlConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("PgsqlPartCodec", new PgsqlRawPartCodec(true, configuration.getFramePartMaxLength()));
        if (MESSAGE_PROCESSING_ACTIVATED) {
            pipeline.addLast("PgsqlPartAggregator",
                    new PgsqlRawPartAggregator(PgsqlSSLRequestMessage.TYPE, PgsqlStartupMessage.TYPE,
                            PgsqlCancelRequestMessage.TYPE, /*PgsqlSimpleQueryMessage.TYPE, */PgsqlParseMessage.TYPE,
                            PgsqlBindMessage.TYPE, PgsqlDescribeMessage.TYPE, PgsqlExecuteMessage.TYPE,
                            PgsqlCloseMessage.TYPE, PgsqlSyncMessage.TYPE, PgsqlFlushMessage.TYPE));
            pipeline.addLast("PgsqlPartAccumulator", new PgsqlRawPartAccumulator(PgsqlSimpleQueryMessage.TYPE));
        }
        if (parserGroup == null) {
            parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        }
        // Session initialization consists of dealing with optional initialization of SSL encryption: a specific SSL handler will be added as first handler in the pipeline if necessary
        // Session initialization ends with the startup message. Then the session initialization handler will be removed
        pipeline.addLast(parserGroup, "SessionInitializationRequestHandler", new SessionInitializationRequestHandler());
        if (MESSAGE_PROCESSING_ACTIVATED) {
            pipeline.addLast(parserGroup, "AuthenticationRequestHandler", new AuthenticationRequestHandler());
            if (QUERY_PROCESSING_ACTIVATED) {
                pipeline.addLast(parserGroup, "QueryRequestHandler", new QueryRequestHandler());
            }
        }
        pipeline.addLast(parserGroup, "PgsqlPartRequestForwarder", new PgsqlRequestForwarder());
    }

}
