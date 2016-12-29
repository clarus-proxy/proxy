package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class NoopEventProcessor implements EventProcessor {

    @Override
    public CString processUserAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters) throws IOException {
		return null;
    }

    @Override
    public int processAuthenticationParameters(ChannelHandlerContext ctx, int authenticationType, ByteBuf specificField) throws IOException {
        return authenticationType;
    }

    @Override
    public CString processAuthentication(ChannelHandlerContext ctx, CString password) throws IOException {
        return null;
    }
    
    @Override
    public QueriesTransferMode<SQLStatement, CString> processStatement(ChannelHandlerContext ctx, SQLStatement sqlStatement) throws IOException {
        return new QueriesTransferMode<>(sqlStatement, TransferMode.FORWARD);
    }

    @Override
    public QueriesTransferMode<BindStep, Void> processBindStep(ChannelHandlerContext ctx, BindStep bindStep) throws IOException {
        return new QueriesTransferMode<>(bindStep, TransferMode.FORWARD);
    }

    @Override
    public QueriesTransferMode<DescribeStep, List<?>[]> processDescribeStep(ChannelHandlerContext ctx, DescribeStep describeStep) throws IOException {
        return new QueriesTransferMode<>(describeStep, TransferMode.FORWARD);
    }

    @Override
    public QueriesTransferMode<ExecuteStep, CString> processExecuteStep(ChannelHandlerContext ctx, ExecuteStep executeStep) throws IOException {
        return new QueriesTransferMode<>(executeStep, TransferMode.FORWARD);
    }

    @Override
    public QueriesTransferMode<CloseStep, Void> processCloseStep(ChannelHandlerContext ctx, CloseStep closeStep) throws IOException {
        return new QueriesTransferMode<>(closeStep, TransferMode.FORWARD);
    }

    @Override
    public QueriesTransferMode<SynchronizeStep, Byte> processSynchronizeStep(ChannelHandlerContext ctx, SynchronizeStep synchronizeStep) throws IOException {
        return new QueriesTransferMode<>(synchronizeStep, TransferMode.FORWARD);
    }

    @Override
    public QueriesTransferMode<FlushStep, Void> processFlushStep(ChannelHandlerContext ctx, FlushStep flushStep) throws IOException {
        return new QueriesTransferMode<>(flushStep, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Void> processParseCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(null, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Void> processBindCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(null, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<List<Long>> processParameterDescriptionResponse(ChannelHandlerContext ctx, List<Long> types) throws IOException {
        return new MessageTransferMode<>(types, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>> processRowDescriptionResponse(ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) throws IOException {
        return new MessageTransferMode<>(fields, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<List<ByteBuf>> processDataRowResponse(ChannelHandlerContext ctx, List<ByteBuf> values) throws IOException {
        return new MessageTransferMode<>(values, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Void> processNoDataResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(null, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<CString> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag) throws IOException {
        return new MessageTransferMode<>(tag, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Void> processEmptyQueryResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(null, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Void> processPortalSuspendedResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(null, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Map<Byte, CString>> processErrorResult(ChannelHandlerContext ctx, Map<Byte, CString> fields) throws IOException {
        return new MessageTransferMode<>(fields, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Void> processCloseCompleteResponse(ChannelHandlerContext ctx) throws IOException {
        return new MessageTransferMode<>(null, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Byte> processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException {
        return new MessageTransferMode<>(transactionStatus, TransferMode.FORWARD);
    }

}
