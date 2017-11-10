package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created on 23/06/2017.
 */
public class GetResponseProcessor implements EventProcessor {

    @Override
    public ProtocolService getProtocolService(ChannelHandlerContext ctx) {
        return null;
    }
}
