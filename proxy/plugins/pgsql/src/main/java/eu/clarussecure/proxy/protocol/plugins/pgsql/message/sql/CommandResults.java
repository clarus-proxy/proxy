package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.List;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage.Field;
import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;

public class CommandResults {
    private boolean parseCompleteRequired;
    private boolean bindCompleteRequired;
    private List<Long> parameterDescription;
    private List<Field> rowDescription;
    private List<List<ByteBuf>> rows;
    private CString completeTag;
    private boolean closeCompleteRequired;

    public CommandResults() {
    }

    public CommandResults(CString completeTag) {
        setCompleteTag(completeTag);
    }

    public boolean isParseCompleteRequired() {
        return parseCompleteRequired;
    }

    public void setParseCompleteRequired(boolean parseCompleteRequired) {
        this.parseCompleteRequired = parseCompleteRequired;
    }

    public boolean isBindCompleteRequired() {
        return bindCompleteRequired;
    }

    public void setBindCompleteRequired(boolean bindCompleteRequired) {
        this.bindCompleteRequired = bindCompleteRequired;
    }

    public List<Long> getParameterDescription() {
        return parameterDescription;
    }

    public void setParameterDescription(List<Long> parameterDescription) {
        this.parameterDescription = parameterDescription;
    }

    public List<Field> getRowDescription() {
        return rowDescription;
    }

    public void setRowDescription(List<Field> rowDescription) {
        this.rowDescription = rowDescription;
    }

    public List<List<ByteBuf>> getRows() {
        return rows;
    }

    public void setRows(List<List<ByteBuf>> rows) {
        this.rows = rows;
    }

    public CString getCompleteTag() {
        return completeTag;
    }

    public void setCompleteTag(CString completeTag) {
        this.completeTag = completeTag;
    }

    public boolean isCloseCompleteRequired() {
        return closeCompleteRequired;
    }

    public void setCloseCompleteRequired(boolean closeCompleteRequired) {
        this.closeCompleteRequired = closeCompleteRequired;
    }

}
