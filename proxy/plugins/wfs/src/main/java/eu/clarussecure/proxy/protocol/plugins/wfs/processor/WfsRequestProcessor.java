package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by administrateur on 23/06/2017.
 */
public class WfsRequestProcessor implements EventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    public void processGetCapabilities(ChannelHandlerContext ctx, HttpObject object) {

    }

    public void processDescribeFeatureType(ChannelHandlerContext ctx, HttpObject object) {

    }

    public void processGetFeature(ChannelHandlerContext ctx, HttpObject object) {

    }

    public void processLockFeature(ChannelHandlerContext ctx, HttpObject object) {

    }

    public void processTransaction(ChannelHandlerContext ctx, HttpObject object) {

    }
}
