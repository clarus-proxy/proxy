package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class PgsqlRowDescriptionMessage extends PgsqlDetailedQueryResponseMessage<List<PgsqlRowDescriptionMessage.Field>> {

    public static final byte TYPE = (byte) 'T';

    public static class Field {

        private CString name;
        private int tableOID;
        private short columnNumber;
        private long typeOID;
        private short typeSize;
        private int typeModifier;
        private short format;

        public Field(CString name, int tableOID, short columnNumber, long typeID, short typeSize, int typeModifier, short format) {
            super();
            this.name = name;
            this.tableOID = tableOID;
            this.columnNumber = columnNumber;
            this.typeOID = typeID;
            this.typeSize = typeSize;
            this.typeModifier = typeModifier;
            this.format = format;
        }

        public CString getName() {
            return name;
        }

        public void setName(CString name) {
            this.name = name;
        }

        public int getTableOID() {
            return tableOID;
        }

        public void setTableOID(int tableOID) {
            this.tableOID = tableOID;
        }

        public short getColumnNumber() {
            return columnNumber;
        }

        public void setColumnNumber(short columnNumber) {
            this.columnNumber = columnNumber;
        }

        public long getTypeOID() {
            return typeOID;
        }

        public void setTypeOID(int typeOID) {
            this.typeOID = typeOID;
        }

        public short getTypeSize() {
            return typeSize;
        }

        public void setTypeSize(short typeSize) {
            this.typeSize = typeSize;
        }

        public int getTypeModifier() {
            return typeModifier;
        }

        public void setTypeModifier(int typeModifier) {
            this.typeModifier = typeModifier;
        }

        public short getFormat() {
            return format;
        }

        public boolean isFormatText() {
            return format == 0;
        }

        public boolean isFormatBinary() {
            return format == 1;
        }

        public void setFormat(short format) {
            this.format = format;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
            builder.append(" [");
            builder.append("name=").append(name);
            builder.append(", tableOID=").append(tableOID);
            builder.append(", columnNumber=").append(columnNumber);
            builder.append(", typeOID=").append(typeOID);
            builder.append(", typeSize=").append(typeSize);
            builder.append(", typeModifier=").append(typeModifier);
            builder.append(", format=").append(format);
            builder.append("]");
            return builder.toString();
        }
    }

    private List<Field> fields;

    public PgsqlRowDescriptionMessage(List<Field> fields) {
        this.fields = Objects.requireNonNull(fields, "fields must not be null");
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = Objects.requireNonNull(fields, "fields must not be null");
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
        builder.append(" [");
        builder.append("fields=");
        int i = 0;
        for (Field field : fields) {
            builder.append(field);
            if (++i < fields.size()) {
                builder.append('\n');
            }
        }
        builder.append("]");
        return builder.toString();
    }

    @Override
    public List<PgsqlRowDescriptionMessage.Field> getDetails() {
        return getFields();
    }
}
