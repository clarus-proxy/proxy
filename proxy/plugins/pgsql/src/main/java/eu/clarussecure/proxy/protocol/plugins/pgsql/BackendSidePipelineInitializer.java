package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandCompleteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDataRowMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlErrorMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlReadyForQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.QueryResponseHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartAggregator;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartCodec;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder.PgsqlResponseForwarder;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class BackendSidePipelineInitializer extends ChannelInitializer<Channel> {

    public BackendSidePipelineInitializer() {
        super();
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(PgsqlConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("PgsqlPartCodec", new PgsqlRawPartCodec(false, configuration.getFramePartMaxLength()));
        EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        String messageProcessing = System.getProperty("pgsql.message.processing");
        if (!(messageProcessing != null && (Boolean.FALSE.toString().equalsIgnoreCase(messageProcessing) || "0".equalsIgnoreCase(messageProcessing) || "no".equalsIgnoreCase(messageProcessing) || "off".equalsIgnoreCase(messageProcessing)))) {
            pipeline.addLast("PgsqlPartAggregator", new PgsqlRawPartAggregator(PgsqlRowDescriptionMessage.TYPE, PgsqlDataRowMessage.TYPE, PgsqlCommandCompleteMessage.TYPE, PgsqlErrorMessage.TYPE, PgsqlReadyForQueryMessage.TYPE));
            String queryProcessing = System.getProperty("pgsql.query.processing");
            if (!(queryProcessing != null && (Boolean.FALSE.toString().equalsIgnoreCase(queryProcessing) || "0".equalsIgnoreCase(queryProcessing) || "no".equalsIgnoreCase(queryProcessing) || "off".equalsIgnoreCase(queryProcessing)))) {
                pipeline.addLast(parserGroup, "QueryResultHandler", new QueryResponseHandler());
            }
        }
        pipeline.addLast(parserGroup, "PgsqlPartResponseForwarder", new PgsqlResponseForwarder());
    }

}
