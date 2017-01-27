package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data;

import org.postgresql.core.Oid;

public enum Type {
    INT2(Oid.INT2, "int2"), // int2
    INT2_ARRAY(Oid.INT2_ARRAY, "_int2", INT2), // _int2
    INT4(Oid.INT4, "int4"), INT4_ARRAY(Oid.INT4_ARRAY, "_int4", INT4), OID(Oid.OID, "oid"), OID_ARRAY(Oid.OID_ARRAY,
            "_oid", OID), INT8(Oid.INT8, "int8"), INT8_ARRAY(Oid.INT8_ARRAY, "_int8", INT8), MONEY(Oid.MONEY,
                    "money"), MONEY_ARRAY(Oid.MONEY_ARRAY, "_money", MONEY), NUMERIC(Oid.NUMERIC,
                            "numeric"), NUMERIC_ARRAY(Oid.NUMERIC_ARRAY, "_numeric", NUMERIC), FLOAT4(Oid.FLOAT4,
                                    "float4"), FLOAT4_ARRAY(Oid.FLOAT4_ARRAY, "_float4", FLOAT4), FLOAT8(Oid.FLOAT8,
                                            "float8"), FLOAT8_ARRAY(Oid.FLOAT8_ARRAY, "_float8", FLOAT8), CHAR(Oid.CHAR,
                                                    "char"), CHAR_ARRAY(Oid.CHAR_ARRAY, "_char", CHAR), BPCHAR(
                                                            Oid.BPCHAR, "bpchar"), BPCHAR_ARRAY(Oid.BPCHAR_ARRAY,
                                                                    "_bpchar", BPCHAR), VARCHAR(Oid.VARCHAR,
                                                                            "varchar"), VARCHAR_ARRAY(Oid.VARCHAR_ARRAY,
                                                                                    "_varchar", VARCHAR), TEXT(Oid.TEXT,
                                                                                            "text"), TEXT_ARRAY(
                                                                                                    Oid.TEXT_ARRAY,
                                                                                                    "_text",
                                                                                                    TEXT), NAME(
                                                                                                            Oid.NAME,
                                                                                                            "name"), NAME_ARRAY(
                                                                                                                    Oid.NAME_ARRAY,
                                                                                                                    "_name",
                                                                                                                    NAME), BYTEA(
                                                                                                                            Oid.BYTEA,
                                                                                                                            "bytea"), BYTEA_ARRAY(
                                                                                                                                    Oid.BYTEA_ARRAY,
                                                                                                                                    "_bytea",
                                                                                                                                    BYTEA), BOOL(
                                                                                                                                            Oid.BOOL,
                                                                                                                                            "bool"), BOOL_ARRAY(
                                                                                                                                                    Oid.BOOL_ARRAY,
                                                                                                                                                    "_bool",
                                                                                                                                                    BOOL), BIT(
                                                                                                                                                            Oid.BIT,
                                                                                                                                                            "bit"), BIT_ARRAY(
                                                                                                                                                                    Oid.BIT_ARRAY,
                                                                                                                                                                    "_bit",
                                                                                                                                                                    BIT), DATE(
                                                                                                                                                                            Oid.DATE,
                                                                                                                                                                            "date"), DATE_ARRAY(
                                                                                                                                                                                    Oid.DATE_ARRAY,
                                                                                                                                                                                    "_date",
                                                                                                                                                                                    DATE), TIME(
                                                                                                                                                                                            Oid.TIME,
                                                                                                                                                                                            "time"), TIME_ARRAY(
                                                                                                                                                                                                    Oid.TIME_ARRAY,
                                                                                                                                                                                                    "_time",
                                                                                                                                                                                                    TIME), TIMETZ(
                                                                                                                                                                                                            Oid.TIMETZ,
                                                                                                                                                                                                            "timetz"), TIMETZ_ARRAY(
                                                                                                                                                                                                                    Oid.TIMETZ_ARRAY,
                                                                                                                                                                                                                    "_timetz",
                                                                                                                                                                                                                    TIMETZ), TIMESTAMP(
                                                                                                                                                                                                                            Oid.TIMESTAMP,
                                                                                                                                                                                                                            "timestamp"), TIMESTAMP_ARRAY(
                                                                                                                                                                                                                                    Oid.TIMESTAMP_ARRAY,
                                                                                                                                                                                                                                    "_timestamp",
                                                                                                                                                                                                                                    TIMESTAMP), TIMESTAMPTZ(
                                                                                                                                                                                                                                            Oid.TIMESTAMPTZ,
                                                                                                                                                                                                                                            "timestampz"), TIMESTAMPTZ_ARRAY(
                                                                                                                                                                                                                                                    Oid.TIMESTAMPTZ_ARRAY,
                                                                                                                                                                                                                                                    "_timestampz",
                                                                                                                                                                                                                                                    TIMESTAMPTZ),
    // Postgis types
    GEOMETRY(-1, "geometry"), GEOMETRY_ARRAY(-1, "_geometry", GEOMETRY), BOX3D(-1, "box3d"), BOX3D_ARRAY(-1, "_box3d",
            BOX3D), BOX2D(-1, "box2d"), BOX2D_ARRAY(-1, "_box2d", BOX2D);

    private long oid;
    private String name;
    private Type elementType;

    private Type(long oid, String name) {
        this(oid, name, null);
    }

    private Type(long oid, String name, Type elementType) {
        this.oid = oid;
        this.name = name;
        this.elementType = elementType;
    }

    public long getOid() {
        return oid;
    }

    public void setOid(long oid) {
        this.oid = oid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getElementType() {
        return elementType;
    }

    public void setElementType(Type elementType) {
        this.elementType = elementType;
    }

    public boolean isPGArray() {
        return elementType != null;
    }

}