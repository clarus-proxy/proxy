package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationHandler extends PgsqlMessageHandler<PgsqlStartupMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationHandler.class);

    public AuthenticationHandler() {
        super(PgsqlStartupMessage.class);
    }

//    @Override
//    protected void decodeStream(ChannelHandlerContext ctx, byte type, MutableByteBufInputStream in) throws IOException {
//        PgsqlStartupMessage pgsqlMsg;
//        // Read content
//        pgsqlMsg = decodeStream(type, in);
//        // Process message
//        process(pgsqlMsg);
//        // Encode content
//        ByteBuf buffer = buffer(in.buffer(), ctx.alloc(), length(pgsqlMsg));
//        buffer = encode(pgsqlMsg, buffer);
//        LastPgsqlRawContent content = new DefaultLastPgsqlRawContent(buffer.slice(pgsqlMsg.getHeaderSize(), buffer.readableBytes() - pgsqlMsg.getHeaderSize()));
//        ctx.fireChannelRead(content);
//    }
//
//    private PgsqlStartupMessage decodeStream(byte type, MutableByteBufInputStream in) throws IOException {
//        assert type == PgsqlStartupMessage.TYPE;
//        // Read protocol
//        int protocolVersion = in.readInt();
//        // Read parameters
//        Map<CString, CString> parameters = new LinkedHashMap<>();
//        CString parameter = CString.valueOf(in.readUntil((byte) 0));
//        while (parameter != null && parameter.length() > 0) {
//            CString value = CString.valueOf(in.readUntil((byte) 0));
//            parameters.put(parameter, value);
//            parameter = CString.valueOf(in.readUntil((byte) 0));
//        }
//        return new PgsqlStartupMessage(protocolVersion, parameters);
//    }
//        

    @Override
    protected PgsqlStartupMessage process(ChannelHandlerContext ctx, PgsqlStartupMessage msg) throws IOException {
        LOGGER.debug("User id: {}, database: {}", msg.getParameters().get(CString.valueOf("user")), msg.getParameters().get(CString.valueOf("database")));
        getEventProcessor(ctx).processAuthentication(ctx, msg.getParameters());
        return msg;
    }

}
