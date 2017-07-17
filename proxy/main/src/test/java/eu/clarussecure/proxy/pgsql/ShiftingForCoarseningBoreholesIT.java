package eu.clarussecure.proxy.pgsql;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import eu.clarussecure.proxy.Proxy;

public class ShiftingForCoarseningBoreholesIT extends ProtectingBoreholes {

    private static final String SECURITY_POLICY = "./src/test/resources/boreholes_3857_coarsening_shift.xml";
    private static final String TARGET = "10.15.0.89";
    private static final String GEOMETRY_TYPE = "POLYGON";
    private static final String PROTECTED_GEOMETRY_TYPE = "POLYGON";

    private static final TableContext BOREHOLES = TableContext.build(1, DATABASE_NAME, SCHEMA_NAME,
            BOREHOLES_TABLE_NAME, BOREHOLES_COLUMN_NAMES, BOREHOLES_WHERE_CLAUSE);

    private static final TableContext GEOMETRY_COLUMNS = TableContext.build(1, DATABASE_NAME, SCHEMA_NAME,
            GEOMETRY_COLUMNS_TABLE_NAME, GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_WHERE_CLAUSE);

    private static Proxy proxy;

    @BeforeClass
    public static void startProxy() throws Exception {
        proxy = startProxy(SECURITY_POLICY, TARGET);
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
