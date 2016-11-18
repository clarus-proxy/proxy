package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.MessageTransferMode;
import io.netty.channel.ChannelHandlerContext;

public class QueryResponseHandler extends PgsqlMessageHandler<PgsqlQueryResponseMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResponseHandler.class);

    public QueryResponseHandler() {
        super(PgsqlParseCompleteMessage.class, PgsqlBindCompleteMessage.class, PgsqlParameterDescriptionMessage.class,
                PgsqlRowDescriptionMessage.class, PgsqlDataRowMessage.class, PgsqlNoDataMessage.class,
                PgsqlCommandCompleteMessage.class, PgsqlEmptyQueryMessage.class, PgsqlPortalSuspendedMessage.class,
                PgsqlErrorMessage.class, PgsqlCloseCompleteMessage.class, PgsqlReadyForQueryMessage.class);
    }

    @Override
    protected PgsqlQueryResponseMessage process(ChannelHandlerContext ctx, PgsqlQueryResponseMessage msg) throws IOException {
        switch (msg.getType()) {
        case PgsqlParseCompleteMessage.TYPE: {
            return process(ctx, (PgsqlParseCompleteMessage)msg,
                    "Parse complete",                                                   // Prefix to use for log messages
                    () -> getEventProcessor(ctx).processParseCompleteResponse(ctx));    // Process the parse complete
        }
        case PgsqlBindCompleteMessage.TYPE: {
            return process(ctx, (PgsqlBindCompleteMessage)msg,
                    "Bind complete",                                                    // Prefix to use for log messages
                    () -> getEventProcessor(ctx).processBindCompleteResponse(ctx));     // Process the bind complete
        }
        case PgsqlParameterDescriptionMessage.TYPE: {
            return processDetails(ctx, (PgsqlParameterDescriptionMessage)msg,
                    "Parameter description",                                                            // Prefix to use for log messages
                    types -> getEventProcessor(ctx).processParameterDescriptionResponse(ctx, types),    // Process the types of the parameter description
                    PgsqlParameterDescriptionMessage::new);                                             // Builder to create a new parameter description message
        }
        case PgsqlRowDescriptionMessage.TYPE: {
            return processDetails(ctx, (PgsqlRowDescriptionMessage)msg,
                    "Row description",                                                                  // Prefix to use for log messages
                    fields -> getEventProcessor(ctx).processRowDescriptionResponse(ctx, fields),        // Process the fields of the row description
                    PgsqlRowDescriptionMessage::new);                                                   // Builder to create a new row description message
        }
        case PgsqlDataRowMessage.TYPE: {
            return processDetails(ctx, (PgsqlDataRowMessage)msg,
                    "Data row",                                                                 // Prefix to use for log messages
                    values -> getEventProcessor(ctx).processDataRowResponse(ctx, values),       // Process the values of the data row
                    PgsqlDataRowMessage::new);                                                  // Builder to create a new data row message
        }
        case PgsqlNoDataMessage.TYPE: {
            return process(ctx, (PgsqlNoDataMessage)msg,
                    "No data",                                                  // Prefix to use for log messages
                    () -> getEventProcessor(ctx).processNoDataResponse(ctx));   // Process the no data
        }
        case PgsqlCommandCompleteMessage.TYPE: {
            return processDetails(ctx, (PgsqlCommandCompleteMessage)msg,
                    "Command complete",                                                         // Prefix to use for log messages
                    fields -> getEventProcessor(ctx).processCommandCompleteResult(ctx, fields), // Process the command result tag
                    PgsqlCommandCompleteMessage::new);                                          // Builder to create a new command complete message
        }
        case PgsqlEmptyQueryMessage.TYPE: {
            return process(ctx, (PgsqlEmptyQueryMessage)msg,
                    "Empty query",                                                      // Prefix to use for log messages
                    () -> getEventProcessor(ctx).processEmptyQueryResponse(ctx));       // Process the empty query
        }
        case PgsqlPortalSuspendedMessage.TYPE: {
            return process(ctx, (PgsqlPortalSuspendedMessage)msg,
                    "Portal suspended",                                                 // Prefix to use for log messages
                    () -> getEventProcessor(ctx).processPortalSuspendedResponse(ctx));  // Process the portal suspended
        }
        case PgsqlErrorMessage.TYPE: {
            PgsqlErrorMessage newMsg = processDetails(ctx, (PgsqlErrorMessage)msg,
                    "Error",                                                            // Prefix to use for log messages
                    fields -> getEventProcessor(ctx).processErrorResult(ctx, fields),   // Process the error fields
                    fields -> new PgsqlErrorMessage(fields));                           // Builder to create a new error message
            responseReceived(ctx);
            return newMsg;
        }
        case PgsqlCloseCompleteMessage.TYPE: {
            return process(ctx, (PgsqlCloseCompleteMessage)msg,
                    "Close complete",                                                   // Prefix to use for log messages
                    () -> getEventProcessor(ctx).processCloseCompleteResponse(ctx));    // Process the close complete
        }
        case PgsqlReadyForQueryMessage.TYPE: {
            PgsqlReadyForQueryMessage newMsg = processDetails(ctx, (PgsqlReadyForQueryMessage)msg,
                    "Ready for query",                                                                  // Prefix to use for log messages
                    trxStatus ->  getEventProcessor(ctx).processReadyForQueryResponse(ctx, trxStatus),  // Process the transaction status
                    PgsqlReadyForQueryMessage::new);                                                    // Builder to create a new ready for query message
            responseReceived(ctx);
            return newMsg;
        }
        default:
            throw new IllegalArgumentException("msg");
        }
    }

    private void responseReceived(ChannelHandlerContext ctx) {
        // Signal response is received
        PgsqlSession psqlSession = getPsqlSession(ctx);
        synchronized(psqlSession) {
            psqlSession.notifyAll();
        }
    }

    @FunctionalInterface
    public interface CheckedSupplier<R> {
       R get() throws IOException;
    }

    private <M extends PgsqlQueryResponseMessage> M process(ChannelHandlerContext ctx, M msg, String prefix, CheckedSupplier<MessageTransferMode<Void>> processor) throws IOException {
        M newMsg = msg;
        LOGGER.debug("{}:", prefix);
        if (!process(ctx, processor)) {
            newMsg = null;
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} dropped", prefix);
            }
        }
        return newMsg;
    }

    private boolean process(ChannelHandlerContext ctx, CheckedSupplier<MessageTransferMode<Void>> processor) throws IOException {
        MessageTransferMode<Void> transferMode = processor.get();
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            return true;
        case FORGET:
            return false;
        case ERROR:
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
        }
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
       R apply(T t) throws IOException;
    }

    private <M extends PgsqlDetailedQueryResponseMessage<D>, D> M processDetails(ChannelHandlerContext ctx, M msg, String prefix, CheckedFunction<D, MessageTransferMode<D>> processor, Function<D, M> builder) throws IOException {
        D details = msg.getDetails();
        M newMsg = msg;
        LOGGER.debug("{}: {}", prefix, details);
        D newDetails = processDetails(ctx, details, processor);
        if (newDetails != details) {
            if (newDetails == null) {
                newMsg = null;
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("{} dropped", prefix);
                }
            } else {
                newMsg = builder.apply(newDetails);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Query response modified: original was {}: {}", prefix, details);
                    LOGGER.trace("Query response modified: new is {}: {}", prefix, newDetails);
                }
            }
        }
        return newMsg;
    }

    private <D> D processDetails(ChannelHandlerContext ctx, D details, CheckedFunction<D, MessageTransferMode<D>> processor) throws IOException {
        D newDetails;
        MessageTransferMode<D> transferMode = processor.apply(details);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newDetails = transferMode.getNewContent();
            break;
        case FORGET:
            newDetails = null;
            break;
        case ERROR:
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": " + transferMode.getTransferMode());
        }

        return newDetails;
    }

}
