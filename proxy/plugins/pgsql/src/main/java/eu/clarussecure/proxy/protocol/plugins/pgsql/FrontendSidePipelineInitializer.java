package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.AuthenticationHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSimpleQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.QueryHandler;
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

    @Override
    protected void initChannel(Channel ch) throws Exception {
        Configuration configuration = ch.attr(PgsqlConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("PgsqlPartCodec", new PgsqlRawPartCodec(true, configuration.getFramePartMaxLength()));
        EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(configuration.getNbParserThreads());
        String messageProcessing = System.getProperty("pgsql.message.processing");
        if (!(messageProcessing != null && (Boolean.FALSE.toString().equalsIgnoreCase(messageProcessing) || "0".equalsIgnoreCase(messageProcessing) || "no".equalsIgnoreCase(messageProcessing) || "off".equalsIgnoreCase(messageProcessing)))) {
            pipeline.addLast("PgsqlPartAggregator", new PgsqlRawPartAggregator(PgsqlStartupMessage.TYPE/*, PgsqlSimpleQueryMessage.TYPE*/));
            pipeline.addLast("PgsqlPartAccumulator", new PgsqlRawPartAccumulator(PgsqlSimpleQueryMessage.TYPE));
            pipeline.addLast(parserGroup, "AuthenticationHandler", new AuthenticationHandler());
            String queryProcessing = System.getProperty("pgsql.query.processing");
            if (!(queryProcessing != null && (Boolean.FALSE.toString().equalsIgnoreCase(queryProcessing) || "0".equalsIgnoreCase(queryProcessing) || "no".equalsIgnoreCase(queryProcessing) || "off".equalsIgnoreCase(queryProcessing)))) {
                pipeline.addLast(parserGroup, "QueryHandler", new QueryHandler());
            }
        }
        pipeline.addLast(parserGroup, "PgsqlPartRequestForwarder", new PgsqlRequestForwarder());
    }

}
