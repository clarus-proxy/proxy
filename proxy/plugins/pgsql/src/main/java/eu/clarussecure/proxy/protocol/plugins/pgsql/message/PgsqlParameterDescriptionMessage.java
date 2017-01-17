package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.List;
import java.util.Objects;

import io.netty.util.internal.StringUtil;

public class PgsqlParameterDescriptionMessage extends PgsqlDetailedQueryResponseMessage<List<Long>> {

    public static final byte TYPE = (byte) 't';

    private List<Long> parameterTypeOIDs;

    public PgsqlParameterDescriptionMessage(List<Long> parameterTypeOIDs) {
        this.parameterTypeOIDs = Objects.requireNonNull(parameterTypeOIDs, "parameterTypeOIDs must not be null");
    }

    public List<Long> getParameterTypeOIDs() {
        return parameterTypeOIDs;
    }

    public void setParameterTypeOIDs(List<Long> parameterTypeOIDs) {
        this.parameterTypeOIDs = Objects.requireNonNull(parameterTypeOIDs, "parameterTypeOIDs must not be null");
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("parameterTypeOIDs=").append(parameterTypeOIDs);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public List<Long> getDetails() {
        return getParameterTypeOIDs();
    }
}
