package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.postgis.Geometry;
import org.postgis.PGbox2d;
import org.postgis.PGbox3d;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.postgis.binary.BinaryParser;
import org.postgresql.jdbc.TimestampUtils;

import eu.clarussecure.proxy.spi.CString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class TypeParser {

    public static Object parse(Type type, int typeModifier, CString value) {
        if (type.isPGArray()) {
            return parsePGArray(value, type, typeModifier);
        } else {
            return parse(value, type, typeModifier);
        }
    }

    private static PGArray parsePGArray(CString value, Type type, int typeModifier) {
        if (value == null) {
            return null;
        }
        int ndims = 1;
        long typeOid = type.getOid();
        int[] dims = null;
        int[] lbounds = null;
        char[] separators = null;
        if (value.length() > 0 && value.charAt(0) == '[') {
            int eq = value.indexOf('=');
            if (eq == -1) {
                throw new IllegalArgumentException(value.toString());
            }
            if (value.charAt(eq - 1) != ']') {
                throw new IllegalArgumentException(value.toString());
            }
            String[] strs = value.substring(0, eq).toString().split("[\\]\\[]");
            ndims = 0;
            for (String str : strs) {
                if (!str.isEmpty()) {
                    ndims++;
                }
            }
            dims = new int[ndims];
            lbounds = new int[ndims];
            separators = new char[ndims];
            int i = 0;
            for (String str : strs) {
                if (!str.isEmpty()) {
                    String[] strs2 = str.split(":");
                    lbounds[i] = Integer.parseInt(strs2[0].toString());
                    dims[i] = Integer.parseInt(strs2[1].toString()) - lbounds[i] + 1;
                }
            }
            //            value = value.substring(eq + 1).trim();
            value = value.substring(eq + 1);
        }
        boolean[] hasnull = new boolean[] { false };
        Object array = parseArray(value, type, typeModifier, hasnull, separators, 0);
        return new PGArray(ndims, hasnull[0], typeOid, dims, lbounds, separators, array);
    }

    private static Object parseArray(CString value, Type type, int typeModifier, boolean[] hasnull, char[] separators,
            int level) {
        if (value.length() < 1 || value.charAt(0) != '{') {
            throw new IllegalArgumentException(value.toString());
        }
        if (value.length() < 2 || value.charAt(value.length() - 1) != '}') {
            throw new IllegalArgumentException(value.toString());
        }
        List<CString> subValues = new ArrayList<>();
        separators[level] = 0;
        int begin = 1;
        boolean insideString = false;
        int insideCurlyBraces = 0;
        for (int i = 1; i < value.length() - 1; i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                i++;
                continue;
            }
            if (insideCurlyBraces == 0) {
                if (!insideString) {
                    if (c == '{') {
                        insideCurlyBraces++;
                    } else if (c == '"') {
                        insideString = true;
                    } else if (c == ',' || c == ';') {
                        separators[level] = c;
                        //                        subValues.add(value.substring(begin, i).trim());
                        subValues.add(value.substring(begin, i));
                        begin = i + 1;
                    }
                } else {
                    if (c == '"') {
                        insideString = false;
                    }
                }
            } else {
                if (c == '{') {
                    insideCurlyBraces++;
                } else if (c == '}') {
                    insideCurlyBraces--;
                }
            }
        }
        if (begin < value.length() - 1) {
            //            subValues.add(value.substring(begin, value.length() - 1).trim());
            subValues.add(value.substring(begin, value.length() - 1));
        }
        Object array = null;
        for (int i = 0; i < subValues.size(); i++) {
            CString subValue = subValues.get(i);
            Object elt;
            if (subValue.charAt(0) == '{') {
                elt = parseArray(subValue, type, typeModifier, hasnull, separators, level + 1);
            } else {
                if (subValue.equalsIgnoreCase("NULL")) {
                    subValue = null;
                } else if (subValue.charAt(0) == '"' && subValue.charAt(subValue.length() - 1) == '"') {
                    subValue = subValue.substring(1, subValue.length() - 1);
                } else {
                    subValue = subValue.replace("\\", "");
                }
                elt = parse(subValue, type, typeModifier);
            }
            if (elt == null) {
                hasnull[0] = true;
            } else if (array == null) {
                array = Array.newInstance(elt.getClass(), subValues.size());
                for (int j = 0; j < i; j++) {
                    Array.set(array, j, null);
                }
            }
            Array.set(array, i, elt);
        }
        return array;
    }

    private static Object parse(CString value, Type type, int typeModifier) {
        if (value == null) {
            return null;
        }
        switch (type) {
        case INT2:
            return parseShort(value);
        case INT4:
            return parseInteger(value);
        case INT8:
            return parseLong(value);
        case TEXT:
            return parseString(value);
        case NUMERIC:
            return parseBigDecimal(value, typeModifier == -1 ? -1 : (typeModifier - 4) & 0xffff);
        case FLOAT4:
            return parseFloat(value);
        case FLOAT8:
            return parseDouble(value);
        case BOOL:
            return parseBoolean(value);
        case DATE:
            return parseDate(value);
        case TIME:
            return parseTime(value);
        case TIMETZ:
            return parseTime(value);
        case TIMESTAMP:
            return parseTimestamp(value);
        case TIMESTAMPTZ:
            return parseTimestamp(value);
        case BYTEA:
            return parseByteArray(value);
        case VARCHAR:
            return parseString(value);
        case OID:
            return (long) (parseInteger(value) & 0xffffl);
        case BPCHAR:
            return parseString(value);
        case MONEY:
            return parseDouble(value);
        case NAME:
            return parseString(value);
        case BIT:
            return parseBoolean(value);
        case CHAR:
            return parseCharacter(value);
        case GEOMETRY:
            return parseGeometry(value);
        case BOX3D:
            return parsePGbox3d(value);
        case BOX2D:
            return parsePGbox2d(value);
        default:
            return parseString(value);
        }
    }

    private static ByteBuf parseByteArray(CString value) {
        int len = value.length() / 2;
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
        ByteBuf buf = allocator.buffer(len);
        for (int i = 0; i < len; i++) {
            int i2 = 2 * i;
            if (i2 + 1 > value.length()) {
                throw new IllegalArgumentException("Hex string has odd length");
            }
            int nib1 = hexToInt(value.charAt(i2));
            int nib0 = hexToInt(value.charAt(i2 + 1));
            byte b = (byte) ((nib1 << 4) + (byte) nib0);
            buf.writeByte(b);
        }
        return buf;
    }

    private static int hexToInt(char hex) {
        int nib = Character.digit(hex, 16);
        if (nib < 0) {
            throw new IllegalArgumentException("Invalid hex digit: '" + hex + "'");
        }
        return nib;
    }

    private static short parseShort(CString value) {
        try {
            return Short.parseShort(value.toString());
        } catch (NumberFormatException e) {
            return parseBigInteger(value, (long) Short.MIN_VALUE & 0xffff, (long) Short.MAX_VALUE & 0xffff)
                    .shortValue();
        }
    }

    private static int parseInteger(CString value) {
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return parseBigInteger(value, (long) Integer.MIN_VALUE & 0xffffffff, (long) Integer.MAX_VALUE & 0xffffffff)
                    .intValue();
        }
    }

    private static long parseLong(CString value) {
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return parseBigInteger(value, Long.MIN_VALUE, Long.MAX_VALUE).longValue();
        }
    }

    private static BigInteger parseBigInteger(CString value, long min, long max) {
        BigInteger i = new BigDecimal(value.toString()).toBigInteger();
        int gt = i.compareTo(BigInteger.valueOf(max));
        int lt = i.compareTo(BigInteger.valueOf(min));

        if (gt > 0 || lt < 0) {
            throw new NumberFormatException(value.toString());
        }
        return i;
    }

    private static float parseFloat(CString value) {
        try {
            return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
            return parseBigDecimal(value, Float.MIN_VALUE, Float.MAX_VALUE).floatValue();
        }
    }

    private static double parseDouble(CString value) {
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return parseBigDecimal(value, Double.MIN_VALUE, Double.MAX_VALUE).doubleValue();
        }
    }

    private static Number parseBigDecimal(CString value, int scale) {
        if (CString.valueOf("NaN").equals(value)) {
            return Double.valueOf(Double.NaN);
        }
        BigDecimal d = new BigDecimal(value.toString());
        if (scale != -1) {
            d = d.setScale(scale);
        }
        return d;
    }

    private static BigDecimal parseBigDecimal(CString value, double min, double max) {
        BigDecimal d = new BigDecimal(value.toString());
        int gt = d.compareTo(BigDecimal.valueOf(max));
        int lt = d.compareTo(BigDecimal.valueOf(min));

        if (gt > 0 || lt < 0) {
            throw new NumberFormatException(value.toString());
        }
        return d;
    }

    private static boolean parseBoolean(CString value) {
        if (value.equalsIgnoreCase("t") || value.equalsIgnoreCase("true") || value.equals("1")) {
            return true;
        } else if (value.equalsIgnoreCase("f") || value.equalsIgnoreCase("false") || value.equals("0")) {
            return false;
        } else {
            try {
                return Double.valueOf(value.toString()).doubleValue() == 1;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    private static char parseCharacter(CString value) {
        return value.charAt(0);
    }

    private static CString parseString(CString value) {
        return value;
    }

    private static Date parseDate(CString value) {
        try {
            TimestampUtils timestampUtils = buildTimestampUtils();
            return timestampUtils.toDate(null, value.toString());
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static Time parseTime(CString value) {
        try {
            TimestampUtils timestampUtils = buildTimestampUtils();
            return timestampUtils.toTime(null, value.toString());
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static Timestamp parseTimestamp(CString value) {
        try {
            TimestampUtils timestampUtils = buildTimestampUtils();
            return timestampUtils.toTimestamp(null, value.toString());
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
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

    private static Geometry parseGeometry(CString value) {
        try {
            return PGgeometry.geomFromString(value.toString());
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static PGbox3d parsePGbox3d(CString value) {
        try {
            String expected = new PGbox3d().getPrefix();
            String actual = value.toString();
            if (actual.startsWith("SRID=")) {
                String[] temp = PGgeometry.splitSRID(actual);
                actual = temp[1].trim();
            }
            int index = actual.indexOf('(');
            if (index == -1) {
                throw new IllegalArgumentException(String.format("value doesn't start with '%s('", expected));
            }
            if (!actual.substring(0, index).trim().equalsIgnoreCase(expected)) {
                throw new IllegalArgumentException(String.format("value doesn't start with '%s'", expected));
            }
            return new PGbox3d(value.toString());
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static PGbox2d parsePGbox2d(CString value) {
        try {
            String expected = new PGbox2d().getPrefix();
            String actual = value.toString();
            if (actual.startsWith("SRID=")) {
                String[] temp = PGgeometry.splitSRID(actual);
                actual = temp[1].trim();
            }
            int index = actual.indexOf('(');
            if (index == -1) {
                throw new IllegalArgumentException(String.format("value doesn't start with '%s('", expected));
            }
            if (!actual.substring(0, index).trim().equalsIgnoreCase(expected)) {
                throw new IllegalArgumentException(String.format("value doesn't start with '%s'", expected));
            }
            return new PGbox2d(value.toString());
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public static Object parse(Type type, int typeModifier, ByteBuf value) {
        if (type.isPGArray()) {
            return parsePGArray(value, type, typeModifier);
        } else {
            return parse(value, type, typeModifier);
        }
    }

    private static PGArray parsePGArray(ByteBuf value, Type type, int typeModifier) {
        if (value == null) {
            return null;
        }
        // Read number of dimensions
        int ndims = value.readInt();
        // Skip NULL flag
        int hasnull = value.readInt();
        // Read element oid
        int eltOid = value.readInt();
        if (eltOid != type.getOid()) {
            throw new IllegalArgumentException("unexpected oid");
        }
        // Read arrays dims and lbounds
        int nbElts = ndims == 0 ? 0 : 1;
        int[] dims = new int[ndims];
        int[] lbounds = new int[ndims];
        for (int i = 0; i < ndims; i++) {
            dims[i] = value.readInt();
            nbElts *= dims[i];
            lbounds[i] = value.readInt();
        }
        Object array = ndims == 0 ? Array.newInstance(getJavaType(type.getElementType()), 0)
                : Array.newInstance(getJavaType(type.getElementType()), dims);
        // Read elements
        Object[] currentArrays = new Object[ndims];
        currentArrays[0] = array;
        for (int i = 1; i < ndims; i++) {
            currentArrays[i] = Array.get(currentArrays[i - 1], 0);
        }
        int[] currentIndexes = new int[ndims];
        Arrays.fill(currentIndexes, 0);
        for (int i = 0; i < nbElts; i++) {
            int eltSize = value.readInt();
            ByteBuf eltValue = value.readSlice(eltSize);
            Object o = parse(eltValue, type, typeModifier);
            Array.set(currentArrays[ndims - 1], currentIndexes[ndims - 1], o);
            for (int j = ndims - 1; j >= 0; j--) {
                currentIndexes[j]++;
                if (currentIndexes[j] == dims[j]) {
                    currentIndexes[j] = 0;
                    continue;
                } else {
                    for (int k = j; k < ndims - 1; k++) {
                        currentArrays[k + 1] = Array.get(currentArrays[k], currentIndexes[k]);
                    }
                    break;
                }
            }
        }

        return new PGArray(ndims, hasnull != 0, eltOid, dims, lbounds, null, array);
    }

    private static Class<?> getJavaType(Type type) {
        switch (type) {
        case INT2:
            return short.class;
        case INT4:
            return int.class;
        case INT8:
            return long.class;
        case TEXT:
            return CString.class;
        case NUMERIC:
            return Number.class;
        case FLOAT4:
            return float.class;
        case FLOAT8:
            return double.class;
        case BOOL:
            return boolean.class;
        case DATE:
            return Date.class;
        case TIME:
            return Time.class;
        case TIMETZ:
            return Time.class;
        case TIMESTAMP:
            return Timestamp.class;
        case TIMESTAMPTZ:
            return Timestamp.class;
        case BYTEA:
            return ByteBuf.class;
        case VARCHAR:
            return CString.class;
        case OID:
            return long.class;
        case BPCHAR:
            return CString.class;
        case MONEY:
            return double.class;
        case NAME:
            return CString.class;
        case BIT:
            return boolean.class;
        case CHAR:
            return char.class;
        case GEOMETRY:
            return Geometry.class;
        case BOX3D:
            return PGbox3d.class;
        case BOX2D:
            return PGbox2d.class;
        default:
            return CString.class;
        }
    }

    private static Object parse(ByteBuf value, Type type, int typeModifier) {
        if (value == null) {
            return null;
        }
        switch (type) {
        case INT2:
            return parseShort(value);
        case INT4:
            return parseInteger(value);
        case INT8:
            return parseLong(value);
        case TEXT:
            return parseString(value);
        case NUMERIC:
            return parseBigDecimal(value, typeModifier == -1 ? -1 : (typeModifier - 4) & 0xffff);
        case FLOAT4:
            return parseFloat(value);
        case FLOAT8:
            return parseDouble(value);
        case BOOL:
            return parseBoolean(value);
        case DATE:
            return parseDate(value);
        case TIME:
            return parseTime(value);
        case TIMETZ:
            return parseTime(value);
        case TIMESTAMP:
            return parseTimestamp(value);
        case TIMESTAMPTZ:
            return parseTimestamp(value);
        case BYTEA:
            return parseByteArray(value);
        case VARCHAR:
            return parseString(value);
        case OID:
            return (long) (parseInteger(value) & 0xffffl);
        case BPCHAR:
            return parseString(value);
        case MONEY:
            return parseDouble(value);
        case NAME:
            return parseString(value);
        case BIT:
            return parseBoolean(value);
        case CHAR:
            return parseCharacter(value);
        case GEOMETRY:
            return parseGeometry(value);
        case BOX3D:
            return parsePGbox3d(value);
        case BOX2D:
            return parsePGbox2d(value);
        default:
            return parseString(value);
        }
    }

    private static ByteBuf parseByteArray(ByteBuf value) {
        return value;
    }

    private static short parseShort(ByteBuf value) {
        return value.readShort();
    }

    private static int parseInteger(ByteBuf value) {
        return value.readInt();
    }

    private static long parseLong(ByteBuf value) {
        return value.readLong();
    }

    private static float parseFloat(ByteBuf value) {
        return value.readFloat();
    }

    private static double parseDouble(ByteBuf value) {
        return value.readDouble();
    }

    private static Number parseBigDecimal(ByteBuf value, int scale) {
        short ndigits = value.readShort();
        short weight = value.readShort();
        short sign = value.readShort();
        short dscale = value.readShort();
        if (sign == 0xC000) {
            return Double.NaN;
        }
        StringBuffer str = new StringBuffer();
        if (sign == 0x4000) {
            str.append('-');
        }
        int i;
        if (weight < 0) {
            str.append('0');
            i = weight + 1;
        } else {
            for (i = 0; i <= weight; i++) {
                short dig = i < ndigits ? value.readShort() : 0;
                boolean append = i > 0;

                short divisor = 1000;
                while (divisor > 1) {
                    short d1 = (short) (dig / divisor);
                    dig -= d1 * divisor;
                    append |= d1 > 0;
                    if (append) {
                        str.append('0' + d1);
                    }
                    divisor /= 10;
                }
                str.append('0' + dig);
            }
        }

        if (dscale > 0) {
            str.append('.');
            for (int j = 0; j < dscale; i++, j += 4) {
                short dig = (i > 0 && i < ndigits) ? value.readShort() : 0;
                boolean append = false;

                short divisor = 1000;
                while (divisor > 1) {
                    short d1 = (short) (dig / divisor);
                    dig -= d1 * divisor;
                    append |= d1 > 0;
                    if (append) {
                        str.append('0' + d1);
                    }
                    divisor /= 10;
                }
                str.append('0' + dig);
            }
        }
        BigDecimal d = new BigDecimal(str.toString());
        if (scale != -1) {
            d = d.setScale(scale);
        }
        return d;
    }

    private static boolean parseBoolean(ByteBuf value) {
        return value.readBoolean();
    }

    private static char parseCharacter(ByteBuf value) {
        return value.readChar();
    }

    private static CString parseString(ByteBuf value) {
        return CString.valueOf(value);
    }

    private static Date parseDate(ByteBuf value) {
        try {
            TimestampUtils timestampUtils = buildTimestampUtils();
            byte[] bytes = new byte[value.readableBytes()];
            value.readBytes(bytes);
            return timestampUtils.toDateBin(null, bytes);
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static Time parseTime(ByteBuf value) {
        try {
            TimestampUtils timestampUtils = buildTimestampUtils();
            byte[] bytes = new byte[value.readableBytes()];
            value.readBytes(bytes);
            return timestampUtils.toTimeBin(null, bytes);
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static Timestamp parseTimestamp(ByteBuf value) {
        try {
            TimestampUtils timestampUtils = buildTimestampUtils();
            byte[] bytes = new byte[value.readableBytes()];
            value.readBytes(bytes);
            return timestampUtils.toTimestampBin(null, bytes, false);
        } catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static Geometry parseGeometry(ByteBuf value) {
        byte[] bytes = new byte[value.readableBytes()];
        value.readBytes(bytes);
        return new BinaryParser().parse(bytes);
    }

    private static PGbox3d parsePGbox3d(ByteBuf value) {
        double xmin = value.readDouble();
        double ymin = value.readDouble();
        double zmin = value.readDouble();
        double xmax = value.readDouble();
        double ymax = value.readDouble();
        double zmax = value.readDouble();
        int srid = value.readInt();
        Point llb = new Point(xmin, ymin, zmin);
        llb.setSrid(srid);
        Point urt = new Point(xmax, ymax, zmax);
        urt.setSrid(srid);
        return new PGbox3d(llb, urt);
    }

    private static PGbox2d parsePGbox2d(ByteBuf value) {
        GBox gbox = parseGBox(value);
        Point llb = new Point(gbox.getXmin(), gbox.getYmin());
        Point urt = new Point(gbox.getXmax(), gbox.getYmax());
        return new PGbox2d(llb, urt);
    }

    private static GBox parseGBox(ByteBuf value) {
        short flags = (short) (value.readByte() & 0xff);
        double xmin = value.readDouble();
        double xmax = value.readDouble();
        double ymin = value.readDouble();
        double ymax = value.readDouble();
        double zmin = value.readDouble();
        double zmax = value.readDouble();
        double mmin = value.readDouble();
        double mmax = value.readDouble();
        return new GBox(flags, xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax);
    }

}
