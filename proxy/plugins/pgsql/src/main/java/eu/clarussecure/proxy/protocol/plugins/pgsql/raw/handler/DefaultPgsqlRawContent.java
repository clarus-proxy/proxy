package eu.clarussecure.proxy.protocol.plugins.pgsql.raw.handler;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.StringUtil;

public class DefaultPgsqlRawContent extends DefaultPgsqlRawPart<PgsqlRawContent> implements PgsqlRawContent {

    /**
     * Creates a new instance with the specified chunk bytes.
     */
    public DefaultPgsqlRawContent(ByteBuf bytes) {
        super(bytes);
    }

    @Override
    public DefaultPgsqlRawContent replace(ByteBuf bytes) {
        return new DefaultPgsqlRawContent(bytes);
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) + "(bytes: " + getBytes().capacity() + ", content: "
                + getContent().readableBytes() + ')';
    }

}
