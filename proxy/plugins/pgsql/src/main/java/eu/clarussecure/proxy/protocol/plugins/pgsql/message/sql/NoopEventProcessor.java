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
    public void processAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters) throws IOException {
    }

    @Override
    public StatementTransferMode processStatement(ChannelHandlerContext ctx, CString statement, boolean lastStatement) throws IOException {
        return new StatementTransferMode(statement, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>> processRowDescriptionResponse(ChannelHandlerContext ctx, List<PgsqlRowDescriptionMessage.Field> fields) {
        return new MessageTransferMode<List<PgsqlRowDescriptionMessage.Field>>(fields, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<List<ByteBuf>> processDataRowResponse(ChannelHandlerContext ctx, List<ByteBuf> values) throws IOException {
        return new MessageTransferMode<List<ByteBuf>>(values, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<CString> processCommandCompleteResult(ChannelHandlerContext ctx, CString tag) throws IOException {
        return new MessageTransferMode<CString>(tag, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Map<Byte, CString>> processErrorResult(ChannelHandlerContext ctx, Map<Byte, CString> fields) throws IOException {
        return new MessageTransferMode<Map<Byte, CString>>(fields, TransferMode.FORWARD);
    }

    @Override
    public MessageTransferMode<Byte> processReadyForQueryResponse(ChannelHandlerContext ctx, Byte transactionStatus) throws IOException {
        return new MessageTransferMode<Byte>(transactionStatus, TransferMode.FORWARD);
    }

}
