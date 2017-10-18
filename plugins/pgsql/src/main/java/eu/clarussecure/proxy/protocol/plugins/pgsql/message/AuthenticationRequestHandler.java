package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.MessageTransferMode;
import eu.clarussecure.proxy.protocol.plugins.tcp.handler.forwarder.DirectedMessage;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationRequestHandler extends PgsqlMessageHandler<PgsqlAuthenticationRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthenticationRequestHandler.class);

    public AuthenticationRequestHandler() {
        super(PgsqlStartupMessage.class, PgsqlPasswordMessage.class);
    }

    @Override
    protected List<DirectedMessage<PgsqlAuthenticationRequest>> directedProcess(ChannelHandlerContext ctx,
            PgsqlAuthenticationRequest msg) throws IOException {
        switch (msg.getType()) {
        case PgsqlStartupMessage.TYPE:
            return process(ctx, (PgsqlStartupMessage) msg, "startup", PgsqlStartupMessage::getParameters,
                    parameters -> getEventProcessor(ctx).processUserIdentification(ctx, parameters),
                    newParameters -> new PgsqlStartupMessage(((PgsqlStartupMessage) msg).getProtocolVersion(),
                            newParameters));
        case PgsqlPasswordMessage.TYPE:
            return process(ctx, (PgsqlPasswordMessage) msg, "password", PgsqlPasswordMessage::getPassword,
                    password -> getEventProcessor(ctx).processUserAuthentication(ctx, password),
                    PgsqlPasswordMessage::new);
        default:
            throw new IllegalArgumentException(String.format("msg type %c", (char) msg.getType()));
        }
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws IOException;
    }

    private <M extends PgsqlAuthenticationRequest, C> List<DirectedMessage<PgsqlAuthenticationRequest>> process(
            ChannelHandlerContext ctx, M msg, String prefix, Function<M, C> supplier,
            CheckedFunction<C, MessageTransferMode<C, Void>> processor, Function<C, M> builder) throws IOException {
        LOGGER.debug("{}:", prefix);
        List<DirectedMessage<PgsqlAuthenticationRequest>> directedMsgs;
        C content = supplier.apply(msg);
        MessageTransferMode<C, Void> transferMode = processor.apply(content);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            if (transferMode.isDirected()) {
                // one specific message for each peer channel
                List<C> newContents = transferMode.getNewDirectedContents();
                directedMsgs = new ArrayList<>(newContents.size());
                for (int i = 0; i < newContents.size(); i++) {
                    C newContent = newContents.get(i);
                    M newMsg = msg;
                    if (content != newContent) {
                        newMsg = builder.apply(newContent);
                        LOGGER.trace("{} modified: original was: {}", prefix, msg);
                        LOGGER.trace("{} modified: new is : {}", prefix, newMsg);
                    }
                    directedMsgs.add(new DirectedMessage<>(i, newMsg));
                }
            } else {
                // one message for all peer channels
                C newContent = transferMode.getNewContent();
                M newMsg = msg;
                if (content != newContent) {
                    newMsg = builder.apply(newContent);
                    LOGGER.trace("{} modified: original was: {}", prefix, msg);
                    LOGGER.trace("{} modified: new is : {}", prefix, newMsg);
                }
                DirectedMessage<PgsqlAuthenticationRequest> directedMsg = new DirectedMessage<>(-1, newMsg);
                directedMsgs = Collections.singletonList(directedMsg);
            }
            break;
        case ERROR:
            sendErrorResponse(ctx, transferMode.getErrorDetails());
            directedMsgs = null;
            LOGGER.trace("{} dropped", prefix);
            break;
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                            + transferMode.getTransferMode());
        }
        return directedMsgs;
    }
}
