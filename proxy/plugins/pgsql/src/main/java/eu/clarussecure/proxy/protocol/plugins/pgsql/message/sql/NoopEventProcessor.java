package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandResultMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.ChannelHandlerContext;

public class NoopEventProcessor implements EventProcessor {

    @Override
    public void processAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters) throws IOException {
    }

    @Override
    public StatementTransferMode processStatement(ChannelHandlerContext ctx, CString statement, boolean lastStatement) throws IOException {
        return new StatementTransferMode(statement, TransferMode.FORWARD);
    }

    @Override
    public CommandResultTransferMode processCommandResult(ChannelHandlerContext ctx, PgsqlCommandResultMessage.Details<?> details) throws IOException {
        return new CommandResultTransferMode(details, TransferMode.FORWARD);
    }

    @Override
    public ReadyForQueryResponseTransferMode processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException {
        return new ReadyForQueryResponseTransferMode(transactionStatus, TransferMode.FORWARD);
    }

}
