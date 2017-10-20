package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public interface EventProcessor {

    MessageTransferMode<Map<CString, CString>, Void> processUserIdentification(ChannelHandlerContext ctx,
            Map<CString, CString> parameters) throws IOException;

    MessageTransferMode<AuthenticationResponse, Void> processAuthenticationResponse(ChannelHandlerContext ctx,
            AuthenticationResponse response) throws IOException;

    MessageTransferMode<CString, Void> processUserAuthentication(ChannelHandlerContext ctx, CString password)
            throws IOException;

    QueriesTransferMode<SQLStatement, CommandResults> processStatement(ChannelHandlerContext ctx,
            SQLStatement sqlStatement) throws IOException;

    QueriesTransferMode<BindStep, CommandResults> processBindStep(ChannelHandlerContext ctx, BindStep bindStep)
            throws IOException;

    QueriesTransferMode<DescribeStep, CommandResults> processDescribeStep(ChannelHandlerContext ctx,
            DescribeStep describeStep) throws IOException;

    QueriesTransferMode<ExecuteStep, CommandResults> processExecuteStep(ChannelHandlerContext ctx,
            ExecuteStep executeStep) throws IOException;

    QueriesTransferMode<CloseStep, CommandResults> processCloseStep(ChannelHandlerContext ctx, CloseStep closeStep)
            throws IOException;

    QueriesTransferMode<SynchronizeStep, Byte> processSynchronizeStep(ChannelHandlerContext ctx,
            SynchronizeStep synchronizeStep) throws IOException;

    QueriesTransferMode<FlushStep, Void> processFlushStep(ChannelHandlerContext ctx, FlushStep flushStep)
            throws IOException;

    MessageTransferMode<Void, Void> processParseCompleteResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<Void, Void> processBindCompleteResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<List<Long>, Void> processParameterDescriptionResponse(ChannelHandlerContext ctx,
            List<Long> types) throws IOException;

    MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>, Void> processRowDescriptionResponse(
            ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) throws IOException;

    MessageTransferMode<List<ByteBuf>, Void> processDataRowResponse(ChannelHandlerContext ctx, List<ByteBuf> values)
            throws IOException;

    MessageTransferMode<Void, Void> processNoDataResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<CString, Void> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag)
            throws IOException;

    MessageTransferMode<Void, Void> processEmptyQueryResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<Void, Void> processPortalSuspendedResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<Map<Byte, CString>, Void> processErrorResult(ChannelHandlerContext ctx,
            Map<Byte, CString> fields) throws IOException;

    MessageTransferMode<Void, Void> processCloseCompleteResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<Byte, Void> processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus)
            throws IOException;

}
