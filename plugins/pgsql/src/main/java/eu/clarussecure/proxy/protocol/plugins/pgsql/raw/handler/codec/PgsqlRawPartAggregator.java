package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.codec;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.FullPgsqlRawMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.LastPgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawContent;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawHeader;
import eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler.PgsqlRawPart;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageAggregator;

public class PgsqlRawPartAggregator
        extends MessageAggregator<PgsqlRawPart, PgsqlRawHeader, PgsqlRawContent, MutablePgsqlRawMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgsqlRawPartAggregator.class);

    private final int[] types;

    private byte currentType = -1;

    public PgsqlRawPartAggregator(int... types) {
        super(Integer.MAX_VALUE);
        this.types = types;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (!super.acceptInboundMessage(msg)) {
            return false;
        }
        if (msg instanceof PgsqlRawHeader) {
            currentType = ((PgsqlRawHeader) msg).getType();
        }
        boolean accept = Arrays.stream(types).anyMatch(type -> currentType == type);
        if (msg instanceof LastPgsqlRawContent) {
            currentType = -1;
        }
        return accept;
    }

    @Override
    protected boolean isStartMessage(PgsqlRawPart msg) throws Exception {
        return msg instanceof PgsqlRawHeader;
    }

    @Override
    protected boolean isContentMessage(PgsqlRawPart msg) throws Exception {
        return msg instanceof PgsqlRawContent;
    }

    @Override
    protected boolean isLastContentMessage(PgsqlRawContent msg) throws Exception {
        return msg instanceof LastPgsqlRawContent;
    }

    @Override
    protected boolean isAggregated(PgsqlRawPart msg) throws Exception {
        return msg instanceof FullPgsqlRawMessage;
    }

    @Override
    protected boolean isContentLengthInvalid(PgsqlRawHeader start, int maxContentLength) throws Exception {
        return start.getTotalLength() > maxContentLength;
    }

    @Override
    protected Object newContinueResponse(PgsqlRawHeader start, int maxContentLength, ChannelPipeline pipeline)
            throws Exception {
        return null;
    }

    @Override
    protected boolean closeAfterContinueResponse(Object msg) throws Exception {
        return false;
    }

    @Override
    protected boolean ignoreContentAfterContinueResponse(Object msg) throws Exception {
        return false;
    }

    @Override
    protected MutablePgsqlRawMessage beginAggregation(PgsqlRawHeader start, ByteBuf content) throws Exception {
        LOGGER.trace("Start aggregation of header raw message {} with content {}...", start, content);
        MutablePgsqlRawMessage message = new DefaultMutablePgsqlRawMessage(content, start.getType(), start.getLength());
        LOGGER.trace("Aggregation of full raw message {} started", message);
        return message;
    }

    @Override
    protected void aggregate(MutablePgsqlRawMessage msg, PgsqlRawContent content) throws Exception {
        LOGGER.trace("Continue aggregation of mutable raw message {} with content {}...", msg, content);
        LOGGER.trace("Aggregation of mutable raw message {} in progress", msg);
    }

    @Override
    protected void finishAggregation(MutablePgsqlRawMessage msg) throws Exception {
        LOGGER.trace("Finish aggregation of mutable raw message {}...", msg);
        LOGGER.trace("Aggregation of mutable raw message {} completed", msg);
    }

}
