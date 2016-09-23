package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCommandResultMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.ChannelHandlerContext;

public interface EventProcessor {

    void processAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters) throws IOException;

    StatementTransferMode processStatement(ChannelHandlerContext ctx, CString statement, boolean lastStatement) throws IOException;

    CommandResultTransferMode processCommandResult(ChannelHandlerContext ctx, PgsqlCommandResultMessage.Details<?> details) throws IOException;

    ReadyForQueryResponseTransferMode processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException;
}
