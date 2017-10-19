package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.StringUtil;

public class PgsqlRowDescriptionMessage
        extends PgsqlDetailedQueryResponseMessage<List<PgsqlRowDescriptionMessage.Field>> {

    public static final byte TYPE = (byte) 'T';

    public static class Field implements ReferenceCounted {

        private CString name;
        private int tableOID;
        private short columnNumber;
        private long typeOID;
        private short typeSize;
        private int typeModifier;
        private short format;

        public Field(CString name) {
            this(name, 0, (short) 0, 0l, (short) 0, 0, (short) 0);
        }

        public Field(CString name, int tableOID, short columnNumber, long typeOID, short typeSize, int typeModifier,
                short format) {
            this.name = Objects.requireNonNull(name, "field name is required");
            this.tableOID = tableOID;
            this.columnNumber = columnNumber;
            this.typeOID = typeOID;
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

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Field other = (Field) obj;
            if (name == null) {
                if (other.name != null) {
                    return false;
                }
            } else if (!name.equals(other.name)) {
                return false;
            }
            return true;
        }

        @Override
        public int refCnt() {
            return name != null ? name.refCnt() : 0;
        }

        @Override
        public ReferenceCounted retain() {
            if (name != null) {
                if (!name.isBuffered()) {
                    // force buffering
                    name.getByteBuf();
                }
                name.retain();
            }
            return this;
        }

        @Override
        public ReferenceCounted retain(int increment) {
            if (name != null) {
                if (!name.isBuffered()) {
                    // force buffering
                    name.getByteBuf();
                }
                return retain(increment);
            }
            return this;
        }

        @Override
        public ReferenceCounted touch() {
            if (name != null) {
                name.touch();
            }
            return this;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            if (name != null) {
                name.touch(hint);
            }
            return this;
        }

        @Override
        public boolean release() {
            boolean released = name == null || name.release();
            if (released) {
                name = null;
            }
            return released;
        }

        @Override
        public boolean release(int decrement) {
            boolean released = name == null || name.release(decrement);
            if (released) {
                name = null;
            }
            return released;
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
