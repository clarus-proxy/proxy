package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.CommandResultTransferMode;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.ReadyForQueryResponseTransferMode;
import io.netty.channel.ChannelHandlerContext;

public class QueryResponseHandler extends PgsqlMessageHandler<PgsqlQueryResponseMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResponseHandler.class);

    public QueryResponseHandler() {
        super(PgsqlCommandCompleteMessage.class, PgsqlErrorMessage.class, PgsqlReadyForQueryMessage.class);
    }

    @Override
    protected PgsqlQueryResponseMessage process(ChannelHandlerContext ctx, PgsqlQueryResponseMessage msg)
            throws IOException {
        switch (msg.getType()) {
        case PgsqlCommandCompleteMessage.TYPE:
        case PgsqlErrorMessage.TYPE: {
            PgsqlCommandResultMessage<?> resultMsg = (PgsqlCommandResultMessage<?>) msg;
            PgsqlCommandResultMessage<?> newMsg = resultMsg;
            PgsqlCommandResultMessage.Details<?> details = resultMsg.getDetails();
            String prefix = resultMsg.getType() == PgsqlCommandCompleteMessage.TYPE ? "Command complete" : "Error";
            LOGGER.debug("{}: {}", prefix, details);
            PgsqlCommandResultMessage.Details<?> newDetails = processCommandResult(ctx, details);
            if (newDetails != details) {
                if (newDetails == null) {
                    newMsg = null;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("{} dropped", prefix);
                    }
                } else {
                    if (newDetails.isDedicatedTo(PgsqlErrorMessage.class)) {
                        newMsg = new PgsqlErrorMessage(newDetails.cast());
                    } else if (newDetails.isDedicatedTo(PgsqlCommandCompleteMessage.class)) {
                        newMsg = new PgsqlCommandCompleteMessage(newDetails.cast());
                    }
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Command result modified: original was {}: {}", prefix, details);
                        prefix = newMsg.getType() == PgsqlCommandCompleteMessage.TYPE ? "Command complete" : "Error";
                        LOGGER.trace("Command result modified: new is {}: {}", prefix, newDetails);
                    }
                }
            }
            return newMsg;
        }
        case PgsqlReadyForQueryMessage.TYPE: {
            PgsqlReadyForQueryMessage readyMsg = (PgsqlReadyForQueryMessage) msg;
            PgsqlReadyForQueryMessage newMsg = readyMsg;
            Byte transactionStatus = readyMsg.getTransactionStatus();
            LOGGER.debug("Ready for query: {}", (char) transactionStatus.byteValue());
            Byte newTransactionStatus = processReadyForQueryResponse(ctx, transactionStatus);
            if (newTransactionStatus != transactionStatus) {
                if (newTransactionStatus == null) {
                    newMsg = null;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Ready for query dropped");
                    }
                } else {
                    newMsg = new PgsqlReadyForQueryMessage(newTransactionStatus);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Ready for query modified: original transaction status was {}",
                                (char) transactionStatus.byteValue());
                        LOGGER.trace("Ready for query modified: new transaction status is {}",
                                (char) newTransactionStatus.byteValue());
                    }
                }
            }
            return newMsg;
        }
        default:
            throw new IllegalArgumentException("msg");
        }
    }

    private PgsqlCommandResultMessage.Details<?> processCommandResult(ChannelHandlerContext ctx, PgsqlCommandResultMessage.Details<?> details) throws IOException {
        PgsqlCommandResultMessage.Details<?> newDetails;
        CommandResultTransferMode transferMode = getEventProcessor(ctx).processCommandResult(ctx, details);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newDetails = transferMode.getNewDetails();
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

    private Byte processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException {
        Byte newTransactionStatus;
        ReadyForQueryResponseTransferMode transferMode = getEventProcessor(ctx).processReadyForQueryResponse(ctx,
                transactionStatus);
        switch (transferMode.getTransferMode()) {
        case FORWARD:
            newTransactionStatus = transferMode.getNewTransactionStatus();
            break;
        case FORGET:
            newTransactionStatus = null;
            break;
        case DENY:
        default:
            // Should not occur
            throw new IllegalArgumentException(
                    "Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
                            + transferMode.getTransferMode());
        }

        return newTransactionStatus;
    }

}
