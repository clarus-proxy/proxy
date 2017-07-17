package eu.clarussecure.proxy.pgsql;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import eu.clarussecure.proxy.Proxy;

public class SplittingBoreholesIT extends ProtectingBoreholes {

    private static final String SECURITY_POLICY = "./src/test/resources/boreholes_3857_splitting.xml";
    private static final String TARGET = "10.15.0.89";
    private static final String[] PROTECTED_DATABASE_NAMES = new String[] { "geodata1", "geodata2" };
    private static final String[] PROTECTED_SCHEMA_NAMES = new String[] { "public", "public" };
    private static final boolean[][] COLUMN_CSPS = new boolean[][] {
            // gid nom_com, adresse, code_bss, denominati, type_point, district,
            // circonscri, precision, altitude, prof_max, geom
            { true, true, false, true, false, true, false, true, false, true, false, true },
            { true, false, true, false, true, false, true, false, true, false, true, true } };
    private static final boolean[] DIFFERENT_VALUE_FLAGS = new boolean[] {
            // gid nom_com, adresse, code_bss, denominati, type_point, district,
            // circonscri, precision, altitude, prof_max, geom
            false, false, false, false, false, false, false, false, false, false, false, true };

    private static final boolean[][] GEOMETRY_COLUMN_CSPS = new boolean[][] {
            // f_table_catalog, f_table_schema, f_table_name, f_geometry_column,
            // coord_dimension, srid, type
            { true, true, true, true, true, true, true }, { true, true, true, true, true, true, true } };
    private static final boolean[] GEOMETRY_COLUMN_DIFFERENT_VALUE_FLAGS = new boolean[] {
            // f_table_catalog, f_table_schema, f_table_name, f_geometry_column,
            // coord_dimension, srid, type
            true, false, false, false, false, false, false };

    private static final String GEOMETRY_TYPE = "POINT";
    private static final String PROTECTED_GEOMETRY_TYPE = "POINT";

    private static final TableContext BOREHOLES = TableContext.build(PROTECTED_DATABASE_NAMES, PROTECTED_SCHEMA_NAMES,
            BOREHOLES_TABLE_NAME, BOREHOLES_COLUMN_NAMES, COLUMN_CSPS, DIFFERENT_VALUE_FLAGS, BOREHOLES_WHERE_CLAUSE);

    private static final TableContext GEOMETRY_COLUMNS = TableContext.build(PROTECTED_DATABASE_NAMES,
            PROTECTED_SCHEMA_NAMES, GEOMETRY_COLUMNS_TABLE_NAME, GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMN_CSPS,
            GEOMETRY_COLUMN_DIFFERENT_VALUE_FLAGS, GEOMETRY_COLUMNS_WHERE_CLAUSE);

    private static Proxy proxy;

    @BeforeClass
    public static void startProxy() throws Exception {
        proxy = startProxy(SECURITY_POLICY, TARGET, TARGET);
    }

    @AfterClass
    public static void stopProxy() throws Exception {
        stopProxy(proxy);
    }

    @Override
    protected TableContext getBoreholes() {
        return BOREHOLES;
    }

    @Override
    protected TableContext getGeometryColumns() {
        return GEOMETRY_COLUMNS;
    }

    @Override
    protected String getGeometryType() {
        return GEOMETRY_TYPE;
    }

    @Override
    protected String getProtectedGeometryType() {
        return PROTECTED_GEOMETRY_TYPE;
    }

}
