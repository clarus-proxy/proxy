package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlBindCompleteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCloseCompleteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandCompleteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDataRowMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlEmptyQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlErrorMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlNoDataMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlNoticeMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParameterDescriptionMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParameterStatusMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParseCompleteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlPortalSuspendedMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlReadyForQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.QueryResponseHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.SessionInitializationResponseHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartAggregator;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartCodec;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder.PgsqlResponseForwarder;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class BackendSidePipelineInitializer extends ChannelInitializer<Channel> {

    private static boolean MESSAGE_PROCESSING_ACTIVATED;
    private static boolean QUERY_PROCESSING_ACTIVATED;
    static {
        String messageProcessing = System.getProperty("pgsql.message.processing", "true");
        MESSAGE_PROCESSING_ACTIVATED = Boolean.TRUE.toString().equalsIgnoreCase(messageProcessing) || "1".equalsIgnoreCase(messageProcessing) || "yes".equalsIgnoreCase(messageProcessing) || "on".equalsIgnoreCase(messageProcessing);
        String queryProcessing = System.getProperty("pgsql.query.processing", "true");
        QUERY_PROCESSING_ACTIVATED = Boolean.TRUE.toString().equalsIgnoreCase(queryProcessing) || "1".equalsIgnoreCase(queryProcessing) || "yes".equalsIgnoreCase(queryProcessing) || "on".equalsIgnoreCase(queryProcessing);
    }

    public BackendSidePipelineInitializer() {
        super();
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(PgsqlConstants.CONFIGURATION_KEY).get();
        PgsqlSession session = (PgsqlSession) ch.attr(TCPConstants.SESSION_KEY).get();
        PgsqlRawPartCodec clientSideCodec = (PgsqlRawPartCodec) session.getClientSideChannel().pipeline().get("PgsqlPartCodec");
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("PgsqlPartCodec", new PgsqlRawPartCodec(false, configuration.getFramePartMaxLength(), clientSideCodec));
        if (MESSAGE_PROCESSING_ACTIVATED) {
            pipeline.addLast("PgsqlPartAggregator", new PgsqlRawPartAggregator(
                    PgsqlParseCompleteMessage.TYPE, PgsqlBindCompleteMessage.TYPE,
                    PgsqlParameterDescriptionMessage.TYPE, PgsqlParameterStatusMessage.TYPE,
                    PgsqlRowDescriptionMessage.TYPE, PgsqlDataRowMessage.TYPE, PgsqlNoDataMessage.TYPE,
                    PgsqlCommandCompleteMessage.TYPE, PgsqlEmptyQueryMessage.TYPE, PgsqlPortalSuspendedMessage.TYPE,
                    PgsqlErrorMessage.TYPE, PgsqlCloseCompleteMessage.TYPE, PgsqlReadyForQueryMessage.TYPE,
                    PgsqlNoticeMessage.TYPE));
        }
        EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        // Session initialization consists of dealing with optional initialization of SSL encryption: a specific SSL handler will be added as first handler in the pipeline if necessary
        // The session initialization handler will be removed while dealing with the startup message (by the SessionInitializationRequestHandler running on the frontend side). 
        pipeline.addLast(parserGroup, "SessionInitializationResponseHandler", new SessionInitializationResponseHandler());
        if (MESSAGE_PROCESSING_ACTIVATED) {
            if (QUERY_PROCESSING_ACTIVATED) {
                pipeline.addLast(parserGroup, "QueryResultHandler", new QueryResponseHandler());
            }
        }
        pipeline.addLast(parserGroup, "PgsqlPartResponseForwarder", new PgsqlResponseForwarder());
    }

}
