package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import org.postgis.Geometry;
import org.postgis.PGbox2d;
import org.postgis.PGbox3d;
import org.postgis.binary.BinaryWriter;
import org.postgresql.jdbc.TimestampUtils;

import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;

public class TypeWriter {

    public static CString toCString(Type type, Object value) {
        if (type.isPGArray()) {
            return toCString((PGArray) value, type);
        } else {
            return toCString(value, type);
        }
    }

    private static CString toCString(PGArray pgArray, Type type) {
        if (pgArray == null) {
            return null;
        }
        if (pgArray.getDims() != null && pgArray.getLbounds() != null) {
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < pgArray.getNdims(); i++) {
                int lbound = pgArray.getLbounds()[i];
                int dim = pgArray.getDims()[i];
                buf.append('[').append(lbound).append(':').append(lbound + dim - 1).append(']');
            }
            buf.append('=');
            CString values = toCString(pgArray.getArray(), type, pgArray.getSeparators(), 0);
            buf.append(values);
            return CString.valueOf(buf);
        } else {
            return toCString(pgArray.getArray(), type, pgArray.getSeparators(), 0);
        }
    }

    private static CString toCString(Object array, Type type, char[] separators, int level) {
        StringBuffer buf = new StringBuffer();
        buf.append('{');
        if (array != null) {
            char separator = separators != null ? separators[level] : ',';
            int len = Array.getLength(array);
            for (int i = 0; i < len; i++) {
                if (i > 0) {
                    buf.append(separator);
                }
                Object value = Array.get(array, i);
                if (value == null) {
                    buf.append("NULL");
                } else if (value.getClass().isArray()) {
                    buf.append(toCString(array, type, separators, level));
                } else if (value instanceof CString) {
                    // TODO Escape ?
                    buf.append('"').append(value).append('"');
                } else {
                    // TODO Escape ?
                    buf.append(value);
                }
            }
        }
        buf.append('}');
        return CString.valueOf(buf);
    }

    private static CString toCString(Object value, Type type) {
        if (value == null) {
            return null;
        }
        switch (type) {
        case INT2:
            return toCString((short) value);
        case INT4:
            return toCString((int) value);
        case INT8:
            return toCString((long) value);
        case TEXT:
            return toCString((CString) value);
        case NUMERIC:
            return toCString((Number) value);
        case FLOAT4:
            return toCString((float) value);
        case FLOAT8:
            return toCString((double) value);
        case BOOL:
            return toCString((boolean) value);
        case DATE:
            return toCString((Date) value);
        case TIME:
            return toCString((Time) value);
        case TIMETZ:
            return toCString((Time) value);
        case TIMESTAMP:
            return toCString((Timestamp) value);
        case TIMESTAMPTZ:
            return toCString((Timestamp) value);
        case BYTEA:
            return toCString((ByteBuf) value);
        case VARCHAR:
            return toCString((CString) value);
        case OID:
            return toCString((long) value & 0xffffl);
        case BPCHAR:
            return toCString((CString) value);
        case MONEY:
            return toCString((double) value);
        case NAME:
            return toCString((CString) value);
        case BIT:
            return toCString((boolean) value);
        case CHAR:
            return toCString((char) value);
        case GEOMETRY:
            return toCString((Geometry) value);
        case BOX3D:
            return toCString((PGbox3d) value);
        case BOX2D:
            return toCString((PGbox2d) value);
        default:
            return toCString((CString) value);
        }
    }

    private static CString toCString(ByteBuf buf) {
        return CString.valueOf(ByteBufUtil.hexDump(buf, 0, buf.capacity()));
    }

    private static CString toCString(short s) {
        return CString.valueOf(Short.toString(s));
    }

    private static CString toCString(int i) {
        return CString.valueOf(Integer.toString(i));
    }

    private static CString toCString(long l) {
        return CString.valueOf(Long.toString(l));
    }

    private static CString toCString(float f) {
        return CString.valueOf(Float.toString(f));
    }

    private static CString toCString(double f) {
        return CString.valueOf(Double.toString(f));
    }

    private static CString toCString(Number n) {
        return CString.valueOf(n.toString());
    }

    private static CString toCString(boolean b) {
        return CString.valueOf(b ? "t" : "f");
    }

    private static CString toCString(char c) {
        return CString.valueOf(Character.toString(c));
    }

    private static CString toCString(CString str) {
        return str;
    }

    private static CString toCString(Date date) {
        TimestampUtils timestampUtils = buildTimestampUtils();
        return CString.valueOf(timestampUtils.toString(null, date));
    }

    private static CString toCString(Time time) {
        TimestampUtils timestampUtils = buildTimestampUtils();
        return CString.valueOf(timestampUtils.toString(null, time));
    }

    private static CString toCString(Timestamp timestamp) {
        TimestampUtils timestampUtils = buildTimestampUtils();
        return CString.valueOf(timestampUtils.toString(null, timestamp));
    }

    private static TimestampUtils buildTimestampUtils() {
        try {
            Constructor<TimestampUtils> constructor = TimestampUtils.class.getConstructor(boolean.class, boolean.class);
            constructor.setAccessible(true);
            TimestampUtils timestampUtils = constructor.newInstance(true, true);
            return timestampUtils;
        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static CString toCString(Geometry geometry) {
        return CString.valueOf(geometry.toString());
    }

    private static CString toCString(PGbox3d pgBox3d) {
        return CString.valueOf(pgBox3d.toString());
    }

    private static CString toCString(PGbox2d pgBox2d) {
        return CString.valueOf(pgBox2d.toString());
    }

    public static ByteBuf getBytes(Type type, Object value) {
        if (type.isPGArray()) {
            return getBytes((PGArray) value, type);
        } else {
            return getBytes(value, type);
        }
    }

    private static ByteBuf getBytes(PGArray pgArray, Type type) {
        if (pgArray == null) {
            return null;
        }
        // Compute length
        int len = Integer.BYTES + Integer.BYTES + Integer.BYTES + pgArray.getNdims() * 2 * Integer.BYTES;
        int nbElts = 0;
        if (pgArray.getNdims() > 0) {
            nbElts = 1;
            for (int i = 0; i < pgArray.getNdims(); i++) {
                nbElts *= pgArray.getDims()[i];
            }
            int[] indexes = new int[pgArray.getNdims()];
            Arrays.fill(indexes, 0);
            for (int i = 0; i < nbElts; i++) {
                int n = i;
                Object elt = pgArray.getArray();
                for (int j = 0; j < pgArray.getNdims(); j++) {
                    int k = n / pgArray.getDims()[j];
                    elt = Array.get(elt, k);
                    n -= k * pgArray.getDims()[j];
                }
                int eltSize = getLength(elt, type.getElementType());
                len += Integer.BYTES + eltSize;
            }
        }
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(len);
        // Write number of dimensions
        buf.writeInt(pgArray.getNdims());
        // Write NULL flag
        buf.writeInt(pgArray.isHasnull() ? 1 : 0);
        // Write element oid
        buf.writeInt((int) pgArray.getTypeOid());
        // Write arrays dims and lbounds
        for (int i = 0; i < pgArray.getNdims(); i++) {
            buf.writeInt(pgArray.getDims()[i]);
            buf.writeInt(pgArray.getLbounds()[i]);
        }
        // Write elements
        for (int i = 0; i < nbElts; i++) {
            int n = i;
            Object elt = pgArray.getArray();
            for (int j = 0; j < pgArray.getNdims(); j++) {
                int k = n / pgArray.getDims()[j];
                elt = Array.get(elt, k);
                n -= k * pgArray.getDims()[j];
            }
            // get bytes
            ByteBuf bytes = getBytes(elt, type.getElementType());
            // Write element length
            buf.writeInt(bytes == null ? 0 : bytes.capacity());
            if (bytes != null) {
                // Write element bytes
                buf.writeBytes(bytes);
            }
        }
        return buf;
    }

    private static int getLength(Object value, Type type) {
        if (value == null) {
            return 0;
        }
        switch (type) {
        case INT2:
            return getLength((short) value);
        case INT4:
            return getLength((int) value);
        case INT8:
            return getLength((long) value);
        case TEXT:
            return getLength((CString) value);
        case NUMERIC:
            return getLength((Number) value);
        case FLOAT4:
            return getLength((float) value);
        case FLOAT8:
            return getLength((double) value);
        case BOOL:
            return getLength((boolean) value);
        case DATE:
            return getLength((Date) value);
        case TIME:
            return getLength((Time) value);
        case TIMETZ:
            return getLength((Time) value);
        case TIMESTAMP:
            return getLength((Timestamp) value);
        case TIMESTAMPTZ:
            return getLength((Timestamp) value);
        case BYTEA:
            return getLength((ByteBuf) value);
        case VARCHAR:
            return getLength((CString) value);
        case OID:
            return getLength((long) value & 0xffffl);
        case BPCHAR:
            return getLength((CString) value);
        case MONEY:
            return getLength((double) value);
        case NAME:
            return getLength((CString) value);
        case BIT:
            return getLength((boolean) value);
        case CHAR:
            return getLength((char) value);
        case GEOMETRY:
            return getLength((Geometry) value);
        case BOX3D:
            return getLength((PGbox3d) value);
        case BOX2D:
            return getLength((PGbox2d) value);
        default:
            return getLength((CString) value);
        }
    }

    private static ByteBuf getBytes(Object value, Type type) {
        if (value == null) {
            return null;
        }
        switch (type) {
        case INT2:
            return getBytes((short) value);
        case INT4:
            return getBytes((int) value);
        case INT8:
            return getBytes((long) value);
        case TEXT:
            return getBytes((CString) value);
        case NUMERIC:
            return getBytes((Number) value);
        case FLOAT4:
            return getBytes((float) value);
        case FLOAT8:
            return getBytes((double) value);
        case BOOL:
            return getBytes((boolean) value);
        case DATE:
            return getBytes((Date) value);
        case TIME:
            return getBytes((Time) value);
        case TIMETZ:
            return getBytes((Time) value);
        case TIMESTAMP:
            return getBytes((Timestamp) value);
        case TIMESTAMPTZ:
            return getBytes((Timestamp) value);
        case BYTEA:
            return getBytes((ByteBuf) value);
        case VARCHAR:
            return getBytes((CString) value);
        case OID:
            return getBytes((int) ((long) value & 0xffffl));
        case BPCHAR:
            return getBytes((CString) value);
        case MONEY:
            return getBytes((double) value);
        case NAME:
            return getBytes((CString) value);
        case BIT:
            return getBytes((boolean) value);
        case CHAR:
            return getBytes((char) value);
        case GEOMETRY:
            return getBytes((Geometry) value);
        case BOX3D:
            return getBytes((PGbox3d) value);
        case BOX2D:
            return getBytes((PGbox2d) value);
        default:
            return getBytes((CString) value);
        }
    }

    private static int getLength(ByteBuf buf) {
        return buf != null ? buf.readableBytes() : 0;
    }

    private static ByteBuf getBytes(ByteBuf bytes) {
        return bytes;
    }

    private static int getLength(short s) {
        return Short.BYTES;
    }

    private static ByteBuf getBytes(short s) {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(getLength(s));
        buf.writeShort(s);
        return buf;
    }

    private static int getLength(int i) {
        return Integer.BYTES;
    }

    private static ByteBuf getBytes(int i) {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(getLength(i));
        buf.writeInt(i);
        return buf;
    }

    private static int getLength(long l) {
        return Long.BYTES;
    }

    private static ByteBuf getBytes(long l) {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(getLength(l));
        buf.writeLong(l);
        return buf;
    }

    private static int getLength(float f) {
        return Float.BYTES;
    }

    private static ByteBuf getBytes(float f) {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(getLength(f));
        buf.writeFloat(f);
        return buf;
    }

    private static int getLength(double d) {
        return Double.BYTES;
    }

    private static ByteBuf getBytes(double d) {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(getLength(d));
        buf.writeDouble(d);
        return buf;
    }

    private static int getLength(Number n) {
        // TODO
        return 0;
    }

    private static ByteBuf getBytes(Number n) {
        // TODO
        return null;
    }

    private static int getLength(boolean b) {
        return Byte.BYTES;
    }

    private static ByteBuf getBytes(boolean b) {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(getLength(b));
        buf.writeBoolean(b);
        return buf;
    }

    private static int getLength(char c) {
        return Byte.BYTES;
    }

    private static ByteBuf getBytes(char c) {
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(getLength(c));
        buf.writeChar(c);
        return buf;
    }

    private static int getLength(CString s) {
        return s != null ? s.length() : 0;
    }

    private static ByteBuf getBytes(CString s) {
        return s.getByteBuf();
    }

    private static int getLength(Date date) {
        // TODO
        return 0;
    }

    private static ByteBuf getBytes(Date date) {
        // TODO
        return null;
    }

    private static int getLength(Time time) {
        // TODO
        return 0;
    }

    private static ByteBuf getBytes(Time time) {
        // TODO
        return null;
    }

    private static int getLength(Timestamp timestamp) {
        // TODO
        return 0;
    }

    private static ByteBuf getBytes(Timestamp timestamp) {
        // TODO
        return null;
    }

    private static int getLength(Geometry geometry) {
        return new BinaryWriter() {
            public int getLength(Geometry geometry) {
                return estimateBytes(geometry);
            }
        }.getLength(geometry);
    }

    private static ByteBuf getBytes(Geometry geometry) {
        byte[] bytes = new BinaryWriter().writeBinary(geometry);
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    private static int getLength(PGbox3d pgbox3d) {
        return 6 * Double.BYTES;
    }

    private static ByteBuf getBytes(PGbox3d pgbox3d) {
        // Compute length
        int len = getLength(pgbox3d);
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(len);
        // Write xmin & ymin & zmin
        buf.writeDouble(pgbox3d.getLLB().getX());
        buf.writeDouble(pgbox3d.getLLB().getY());
        buf.writeDouble(pgbox3d.getLLB().getZ());
        // Write xmax & ymax & zmax
        buf.writeDouble(pgbox3d.getURT().getX());
        buf.writeDouble(pgbox3d.getURT().getY());
        buf.writeDouble(pgbox3d.getURT().getZ());
        return buf;
    }

    private static int getLength(GBox gbox) {
        return Byte.BYTES + 8 * Double.BYTES;
    }

    private static ByteBuf getBytes(GBox gbox) {
        // Compute length
        int len = getLength(gbox);
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(len);
        // Write flags
        buf.writeByte((byte) gbox.getFlags());
        // Write xmin & xmax
        buf.writeDouble(gbox.getXmin());
        buf.writeDouble(gbox.getXmax());
        // Write ymin & ymax
        buf.writeDouble(gbox.getYmin());
        buf.writeDouble(gbox.getYmax());
        // Write zmin & zmax
        buf.writeDouble(gbox.getZmin());
        buf.writeDouble(gbox.getZmax());
        // Write mmin & mmax
        buf.writeDouble(gbox.getMmin());
        buf.writeDouble(gbox.getMmax());
        return buf;
    }

    private static int getLength(PGbox2d pgbox2d) {
        return getLength((GBox) null);
    }

    private static ByteBuf getBytes(PGbox2d pgbox2d) {
        GBox gbox = new GBox((short) 0, pgbox2d.getLLB().getX(), pgbox2d.getURT().getX(), pgbox2d.getLLB().getY(),
                pgbox2d.getURT().getY(), 0, 0, 0, 0);
        return getBytes(gbox);
    }

}
