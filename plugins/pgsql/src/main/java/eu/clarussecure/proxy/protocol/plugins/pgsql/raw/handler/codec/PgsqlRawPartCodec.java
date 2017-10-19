package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import io.netty.channel.CombinedChannelDuplexHandler;

public class PgsqlRawPartCodec extends CombinedChannelDuplexHandler<PgsqlRawPartDecoder, PgsqlRawPartEncoder> {

    public PgsqlRawPartCodec(boolean frontend, int maxlen) {
        super(new PgsqlRawPartDecoder(frontend, maxlen), new PgsqlRawPartEncoder(frontend));
    }

    public void skipFirstMessages() {
        inboundHandler().skipFirstMessages();
    }
}
