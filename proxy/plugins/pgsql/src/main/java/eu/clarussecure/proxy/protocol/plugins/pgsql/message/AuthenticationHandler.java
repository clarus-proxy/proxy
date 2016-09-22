package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlUtilities;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.AuthenticationProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlMessageHandler;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationHandler extends PgsqlMessageHandler<PgsqlStartupMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationHandler.class);

    public AuthenticationHandler() {
        super(PgsqlStartupMessage.TYPE);
    }

//    @Override
//    protected void decodeStream(ChannelHandlerContext ctx, byte type, MutableByteBufInputStream in) throws IOException {
//        PgsqlStartupMessage pgsqlMsg;
//        // Read content
//        pgsqlMsg = decodeStream(type, in);
//        // Process message
//        process(pgsqlMsg);
//        // Encode content
//        ByteBufAllocator allocator = new RecyclerByteBufAllocator(in.buffer(), ctx);
//        ByteBuf byteBuf = encode(pgsqlMsg, allocator);
//        LastPgsqlRawContent content = new DefaultLastPgsqlRawContent(byteBuf.slice(pgsqlMsg.getHeaderSize(), byteBuf.readableBytes() - pgsqlMsg.getHeaderSize()));
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
    protected PgsqlStartupMessage decode(ChannelHandlerContext ctx, byte type, ByteBuf content) throws IOException {
        assert type == PgsqlStartupMessage.TYPE;
        // Read protocol
        int protocolVersion = content.readInt();
        // Read parameters
        Map<CString, CString> parameters = new LinkedHashMap<>();
        CString parameter = PgsqlUtilities.getCString(content);
        if (parameter == null) {
            throw new IOException("unexpected end of message");
        }
        while (parameter.length() > 0) {
            CString value = PgsqlUtilities.getCString(content);
            parameters.put(parameter, value);
            parameter = PgsqlUtilities.getCString(content);
        }
        return new PgsqlStartupMessage(protocolVersion, parameters);
    }

    @Override
    protected PgsqlStartupMessage process(ChannelHandlerContext ctx, PgsqlStartupMessage msg) throws IOException {
        LOGGER.debug("User id: {}, database: {}", msg.getParameters().get(CString.valueOf("user")), msg.getParameters().get(CString.valueOf("database")));
        getAuthenticationProcessor(ctx).processAuthentication(ctx, msg.getParameters());
        return msg;
    }

    @Override
    protected ByteBuf encode(ChannelHandlerContext ctx, PgsqlStartupMessage msg, ByteBufAllocator allocator) throws IOException {
        // Compute total length
        int total = msg.getHeaderSize() + Integer.BYTES;
        for (Map.Entry<CString, CString> parameter : msg.getParameters().entrySet()) {
            total += parameter.getKey().clen();
            total += parameter.getValue().clen();
        }
        total += Byte.BYTES;
        // Allocate buffer
        ByteBuf buffer = allocator.buffer(total);
        // Write header (length)
        int len = total;
        buffer.writeInt(len);
        // Write protocol
        buffer.writeInt(msg.getProtocolVersion());
        // Write parameters
        for (Map.Entry<CString, CString> parameter : msg.getParameters().entrySet()) {
            ByteBuf key = parameter.getKey().getByteBuf(ctx.alloc());
            ByteBuf value = parameter.getValue().getByteBuf(ctx.alloc());
            buffer.writeBytes(key);
            buffer.writeBytes(value);
            key.release();
            value.release();
        }
        buffer.writeByte(0);
        return buffer;
    }

    private AuthenticationProcessor getAuthenticationProcessor(ChannelHandlerContext ctx) {
        PgsqlSession session = ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return session.getAuthenticationProcessor();
    }

}
