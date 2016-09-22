package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.AuthenticationHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSimpleQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlStartupMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.QueryHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartAccumulator;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartAggregator;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartCodec;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder.PgsqlRequestForwarder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class FrontendPipelineInitializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel ch) throws Exception {
        PgsqlConfiguration configuration = ch.attr(PgsqlConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("PgsqlPartCodec", new PgsqlRawPartCodec(true, configuration.getMessagePartMaxLength()));
        pipeline.addLast("PgsqlPartAggregator", new PgsqlRawPartAggregator(PgsqlStartupMessage.TYPE, PgsqlSimpleQueryMessage.TYPE));
//        pipeline.addLast("PgsqlPartAccumulator", new PgsqlRawPartAccumulator(PgsqlSimpleQueryMessage.TYPE));
        EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors());
        pipeline.addLast(parserGroup, "AuthenticationHandler", new AuthenticationHandler());
        pipeline.addLast(parserGroup, "QueryHandler", new QueryHandler());
        pipeline.addLast(parserGroup, "PgsqlPartRequestForwarder", new PgsqlRequestForwarder());
    }

}
