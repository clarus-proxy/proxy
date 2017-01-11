package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public interface EventProcessor {

    CString processUserAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters) throws IOException;

    int processAuthenticationParameters(ChannelHandlerContext ctx, int authenticationType, ByteBuf specificField) throws IOException;

    CString processAuthentication(ChannelHandlerContext ctx, CString password) throws IOException, NoSuchAlgorithmException;

    QueriesTransferMode<SQLStatement, CommandResults> processStatement(ChannelHandlerContext ctx, SQLStatement sqlStatement) throws IOException;

    QueriesTransferMode<BindStep, CommandResults> processBindStep(ChannelHandlerContext ctx, BindStep bindStep) throws IOException;

    QueriesTransferMode<DescribeStep, CommandResults> processDescribeStep(ChannelHandlerContext ctx, DescribeStep describeStep) throws IOException;

    QueriesTransferMode<ExecuteStep, CommandResults> processExecuteStep(ChannelHandlerContext ctx, ExecuteStep executeStep) throws IOException;

    QueriesTransferMode<CloseStep, CommandResults> processCloseStep(ChannelHandlerContext ctx, CloseStep closeStep) throws IOException;

    QueriesTransferMode<SynchronizeStep, Byte> processSynchronizeStep(ChannelHandlerContext ctx, SynchronizeStep synchronizeStep) throws IOException;

    QueriesTransferMode<FlushStep, Void> processFlushStep(ChannelHandlerContext ctx, FlushStep flushStep) throws IOException;

    MessageTransferMode<Void> processParseCompleteResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<Void> processBindCompleteResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<List<Long>> processParameterDescriptionResponse(ChannelHandlerContext ctx, List<Long> types) throws IOException;

    MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>> processRowDescriptionResponse(ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) throws IOException;

    MessageTransferMode<List<ByteBuf>> processDataRowResponse(ChannelHandlerContext ctx, List<ByteBuf> values) throws IOException;

    MessageTransferMode<Void> processNoDataResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<CString> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag) throws IOException;

    MessageTransferMode<Void> processEmptyQueryResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<Void> processPortalSuspendedResponse(ChannelHandlerContext ctx) throws IOException;

    MessageTransferMode<Map<Byte, CString>> processErrorResult(ChannelHandlerContext ctx, Map<Byte, CString> fields) throws IOException;

    MessageTransferMode<Void>  processCloseCompleteResponse(ChannelHandlerContext ctx)  throws IOException;

    MessageTransferMode<Byte> processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException;

}
