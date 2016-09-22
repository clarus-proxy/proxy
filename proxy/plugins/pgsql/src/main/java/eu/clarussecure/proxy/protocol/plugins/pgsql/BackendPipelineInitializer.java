package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandCompleteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlErrorMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlReadyForQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.QueryResponseHandler;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartAggregator;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec.PgsqlRawPartCodec;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.forwarder.PgsqlResponseForwarder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class BackendPipelineInitializer extends ChannelInitializer<Channel> {

    //private final EventExecutorGroup parserGroup;

    public BackendPipelineInitializer(EventExecutorGroup parserGroup) {
        super();
        //this.parserGroup = parserGroup;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        PgsqlConfiguration configuration = ch.attr(PgsqlConstants.CONFIGURATION_KEY).get();
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("PgsqlPartCodec", new PgsqlRawPartCodec(false, configuration.getMessagePartMaxLength()));
        pipeline.addLast("PgsqlPartAggregator", new PgsqlRawPartAggregator(PgsqlCommandCompleteMessage.TYPE, PgsqlErrorMessage.TYPE, PgsqlReadyForQueryMessage.TYPE));
        EventExecutorGroup parserGroup = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors());
        pipeline.addLast(parserGroup, "QueryResultHandler", new QueryResponseHandler());
        pipeline.addLast(parserGroup, "PgsqlPartResponseForwarder", new PgsqlResponseForwarder());
    }

}
