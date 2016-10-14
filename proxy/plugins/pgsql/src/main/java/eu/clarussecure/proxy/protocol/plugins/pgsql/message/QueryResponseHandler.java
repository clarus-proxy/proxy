package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.MessageTransferMode;
import io.netty.channel.ChannelHandlerContext;

public class QueryResponseHandler extends PgsqlMessageHandler<PgsqlQueryResponseMessage<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResponseHandler.class);

    public QueryResponseHandler() {
        super(PgsqlRowDescriptionMessage.class, PgsqlDataRowMessage.class, PgsqlCommandCompleteMessage.class, PgsqlErrorMessage.class, PgsqlReadyForQueryMessage.class);
    }

    @Override
    protected PgsqlQueryResponseMessage<?> process(ChannelHandlerContext ctx, PgsqlQueryResponseMessage<?> msg)
            throws IOException {
        switch (msg.getType()) {
        case PgsqlRowDescriptionMessage.TYPE: {
            return process(ctx, (PgsqlRowDescriptionMessage)msg,
                    "Row description", // Prefix to use for log messages
                    fields -> getEventProcessor(ctx).processRowDescriptionResponse(ctx, fields), // Process the fields of the row description
                    PgsqlRowDescriptionMessage::new); // Builder to create a new row description message
        }
        case PgsqlDataRowMessage.TYPE: {
            return process(ctx, (PgsqlDataRowMessage)msg,
                    "Data row", // Prefix to use for log messages
                    values -> getEventProcessor(ctx).processDataRowResponse(ctx, values), // Process the values of the data row
                    PgsqlDataRowMessage::new); // Builder to create a new data row message
        }
        case PgsqlCommandCompleteMessage.TYPE: {
            return process(ctx, (PgsqlCommandCompleteMessage)msg,
                    "Command complete", // Prefix to use for log messages
                    fields -> getEventProcessor(ctx).processCommandCompleteResult(ctx, fields), // Process the command result tag
                    PgsqlCommandCompleteMessage::new); // Builder to create a new command complete message
        }
        case PgsqlErrorMessage.TYPE: {
            return process(ctx, (PgsqlErrorMessage)msg,
                    "Error", // Prefix to use for log messages
                    fields -> getEventProcessor(ctx).processErrorResult(ctx, fields), // Process the error fields
                    fields -> new PgsqlErrorMessage(fields)); // Builder to create a new error message
        }
        case PgsqlReadyForQueryMessage.TYPE:
            return process(ctx, (PgsqlReadyForQueryMessage)msg,
                    "Ready for query", // Prefix to use for log messages
                    trxStatus -> getEventProcessor(ctx).processReadyForQueryResponse(ctx, trxStatus), // Process the transaction status
                    PgsqlReadyForQueryMessage::new); // Builder to create a new ready for query message
        default:
            throw new IllegalArgumentException("msg");
        }
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
       R apply(T t) throws IOException;
    }

    private <T extends PgsqlQueryResponseMessage<D>, D> T process(ChannelHandlerContext ctx, T msg, String prefix, CheckedFunction<D, MessageTransferMode<D>> processor, Function<D, T> builder) throws IOException {
        D details = msg.getDetails();
        T newMsg = msg;
        LOGGER.debug("{}: {}", prefix, details);
        D newDetails = process(ctx, details, processor);
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

    private <D> D process(ChannelHandlerContext ctx, D details, CheckedFunction<D, MessageTransferMode<D>> processor) throws IOException {
        D newDetails;
        MessageTransferMode<D> transferMode = processor.apply(details);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newDetails = transferMode.getNewContent();
            break;
        case FORGET:
            newDetails = null;
            break;
        case DENY:
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                            + transferMode.getTransferMode());
        }

        return newDetails;
    }

}
