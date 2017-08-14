package eu.clarussecure.proxy.pgsql;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;

public class ShiftingForCoarseningBoreholesIT extends ProtectingBoreholes {

    private static final String SECURITY_POLICY = "./src/test/resources/boreholes_3857_coarsening_shift.xml";
    private static final String TARGET = "10.15.0.89";
    private static final String GEOMETRY_TYPE = "POLYGON";
    private static final String PROTECTED_GEOMETRY_TYPE = "POLYGON";

    private final DatasetContext boreholes = buildTableContext(1, BOREHOLES_SCRIPT, DATABASE_NAME, SCHEMA_NAME,
            BOREHOLES_TABLE_NAME, BOREHOLES_COLUMN_NAMES, BOREHOLES_WHERE_CLAUSE);

    private final DatasetContext geometryColumns = buildTableContext(1, null, DATABASE_NAME, SCHEMA_NAME,
            GEOMETRY_COLUMNS_TABLE_NAME, GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_WHERE_CLAUSE);

    @ClassRule
    public static RuleChain getRuleChain() {
        ProxyResource proxyResource = new ProxyResource(SECURITY_POLICY, TARGET);
        return getRuleChain(proxyResource, BOREHOLES_SCRIPT, SCHEMA_NAME, BOREHOLES_TABLE_NAME);
    }

    @Override
    protected DatasetContext getBoreholes() {
        return boreholes;
    }

    @Override
    protected DatasetContext getGeometryColumns() {
        return geometryColumns;
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
