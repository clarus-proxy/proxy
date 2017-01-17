package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import io.netty.channel.CombinedChannelDuplexHandler;

public class PgsqlRawPartCodec extends CombinedChannelDuplexHandler<PgsqlRawPartDecoder, PgsqlRawPartEncoder> {

    public PgsqlRawPartCodec(boolean frontend, int maxlen) {
        super(new PgsqlRawPartDecoder(frontend, maxlen), new PgsqlRawPartEncoder(frontend));
    }

    public PgsqlRawPartCodec(boolean frontend, int maxlen, PgsqlRawPartCodec clientSideCodec) {
        this(frontend, maxlen);
        //        associate(clientSideCodec);
    }

    //    public void associate(PgsqlRawPartCodec codec) {
    //        inboundHandler().setOtherSideDecoder(codec.inboundHandler());
    //        codec.inboundHandler().setOtherSideDecoder(inboundHandler());
    //    }

    public void skipFirstMessages() {
        inboundHandler().skipFirstMessages();
    }
}
