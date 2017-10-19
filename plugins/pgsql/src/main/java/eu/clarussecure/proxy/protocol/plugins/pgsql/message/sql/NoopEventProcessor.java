package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class NoopEventProcessor implements EventProcessor {

    @Override
    public MessageTransferMode<Map<CString, CString>, Void> processUserIdentification(ChannelHandlerContext ctx,
            Map<CString, CString> parameters) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, parameters);
    }

    @Override
    public MessageTransferMode<AuthenticationResponse, Void> processAuthenticationResponse(ChannelHandlerContext ctx,
            AuthenticationResponse response) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, response);
    }

    @Override
    public MessageTransferMode<CString, Void> processUserAuthentication(ChannelHandlerContext ctx, CString password)
            throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, password);
    }

    @Override
    public QueriesTransferMode<SQLStatement, CommandResults> processStatement(ChannelHandlerContext ctx,
            SQLStatement sqlStatement) throws IOException {
        return new QueriesTransferMode<>(TransferMode.FORWARD, sqlStatement);
    }

    @Override
    public QueriesTransferMode<BindStep, CommandResults> processBindStep(ChannelHandlerContext ctx, BindStep bindStep)
            throws IOException {
        return new QueriesTransferMode<>(TransferMode.FORWARD, bindStep);
    }

    @Override
    public QueriesTransferMode<DescribeStep, CommandResults> processDescribeStep(ChannelHandlerContext ctx,
            DescribeStep describeStep) throws IOException {
        return new QueriesTransferMode<>(TransferMode.FORWARD, describeStep);
    }

    @Override
    public QueriesTransferMode<ExecuteStep, CommandResults> processExecuteStep(ChannelHandlerContext ctx,
            ExecuteStep executeStep) throws IOException {
        return new QueriesTransferMode<>(TransferMode.FORWARD, executeStep);
    }

    @Override
    public QueriesTransferMode<CloseStep, CommandResults> processCloseStep(ChannelHandlerContext ctx,
            CloseStep closeStep) throws IOException {
        return new QueriesTransferMode<>(TransferMode.FORWARD, closeStep);
    }

    @Override
    public QueriesTransferMode<SynchronizeStep, Byte> processSynchronizeStep(ChannelHandlerContext ctx,
            SynchronizeStep synchronizeStep) throws IOException {
        return new QueriesTransferMode<>(TransferMode.FORWARD, synchronizeStep);
    }

    @Override
    public QueriesTransferMode<FlushStep, Void> processFlushStep(ChannelHandlerContext ctx, FlushStep flushStep)
            throws IOException {
        return new QueriesTransferMode<>(TransferMode.FORWARD, flushStep);
    }

    @Override
    public MessageTransferMode<Void, Void> processParseCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, null);
    }

    @Override
    public MessageTransferMode<Void, Void> processBindCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, null);
    }

    @Override
    public MessageTransferMode<List<Long>, Void> processParameterDescriptionResponse(ChannelHandlerContext ctx,
            List<Long> types) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, Collections.singletonList(types));
    }

    @Override
    public MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>, Void> processRowDescriptionResponse(
            ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, Collections.singletonList(fields));
    }

    @Override
    public MessageTransferMode<List<ByteBuf>, Void> processDataRowResponse(ChannelHandlerContext ctx,
            List<ByteBuf> values) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, Collections.singletonList(values));
    }

    @Override
    public MessageTransferMode<Void, Void> processNoDataResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, null);
    }

    @Override
    public MessageTransferMode<CString, Void> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag)
            throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, tag);
    }

    @Override
    public MessageTransferMode<Void, Void> processEmptyQueryResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, null);
    }

    @Override
    public MessageTransferMode<Void, Void> processPortalSuspendedResponse(ChannelHandlerContext ctx)
            throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, null);
    }

    @Override
    public MessageTransferMode<Map<Byte, CString>, Void> processErrorResult(ChannelHandlerContext ctx,
            Map<Byte, CString> fields) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, fields);
    }

    @Override
    public MessageTransferMode<Void, Void> processCloseCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, null);
    }

    @Override
    public MessageTransferMode<Byte, Void> processReadyForQueryResponse(ChannelHandlerContext ctx,
            Byte transactionStatus) throws IOException {
        return new MessageTransferMode<>(TransferMode.FORWARD, transactionStatus);
    }

}
