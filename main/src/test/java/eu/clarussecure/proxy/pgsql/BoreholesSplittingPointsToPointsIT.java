package eu.clarussecure.proxy.pgsql;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;

public class BoreholesSplittingPointsToPointsIT extends BoreholesProtection {

    private static final String SECURITY_POLICY = "./src/test/resources/boreholes_3857_splitting_points_to_points.xml";

    private static final String BOREHOLES_TABLE_NAME = "tu_split_boreholes_3857_points";
    private static final String[] PROTECTED_DATABASE_NAMES = new String[] { "geodata1", "geodata2" };
    private static final String[] PROTECTED_SCHEMA_NAMES = new String[] { SCHEMA_NAME, SCHEMA_NAME };
    private static final String[] PROTECTED_BOREHOLES_TABLE_NAMES = new String[] { BOREHOLES_TABLE_NAME,
            BOREHOLES_TABLE_NAME };
    private static final String[][] PROTECTED_BOREHOLES_COLUMN_NAMES = new String[][] {
            // gid nom_com, adresse, code_bss, denominati, type_point, district,
            // circonscri, precision, altitude, prof_max, geom
            { "gid", "nom_com", null, "code_bss", null, "type_point", null, "circonscri", null, "altitude", null,
                    "geom" },
            { "gid", null, "adresse", null, "denominati", null, "district", null, "precision", null, "prof_max",
                    "geom" } };
    private static final boolean[] BOREHOLES_COLUMN_PROTECTION_FLAGS = new boolean[] {
            // gid nom_com, adresse, code_bss, denominati, type_point, district,
            // circonscri, precision, altitude, prof_max, geom
            false, false, false, false, false, false, false, false, false, false, false, true };

    private static final String[] PROTECTED_GEOMETRY_COLUMNS_TABLE_NAMES = new String[] { GEOMETRY_COLUMNS_TABLE_NAME,
            GEOMETRY_COLUMNS_TABLE_NAME };
    private static final String[][] PROTECTED_GEOMETRY_COLUMNS_COLUMN_NAMES = new String[][] {
            { "f_table_catalog", "f_table_schema", "f_table_name", "f_geometry_column", "coord_dimension", "srid",
                    "type" },
            { "f_table_catalog", "f_table_schema", "f_table_name", "f_geometry_column", "coord_dimension", "srid",
                    "type" } };
    private static final boolean[] GEOMETRY_COLUMNS_COLUMN_PROTECTION_FLAGS = new boolean[] {
            // f_table_catalog, f_table_schema, f_table_name, f_geometry_column,
            // coord_dimension, srid, type
            true, false, false, false, false, false, false };
    private static final String GEOMETRY_COLUMNS_WHERE_CLAUSE = "f_table_name = '" + BOREHOLES_TABLE_NAME + "'";

    private static final String GEOMETRY_TYPE = "POINT";
    private static final String PROTECTED_GEOMETRY_TYPE = "POINT";

    private final DatasetContext boreholes = buildTableContext(BOREHOLES_SCRIPT, BOREHOLES_TABLE_NAME_IN_SCRIPT,
            DATABASE_NAME, SCHEMA_NAME, BOREHOLES_TABLE_NAME, BOREHOLES_COLUMN_NAMES, BOREHOLES_COLUMN_PROTECTION_FLAGS,
            PROTECTED_DATABASE_NAMES, PROTECTED_SCHEMA_NAMES, PROTECTED_BOREHOLES_TABLE_NAMES,
            PROTECTED_BOREHOLES_COLUMN_NAMES, BOREHOLES_WHERE_CLAUSE);

    private final DatasetContext geometryColumns = buildTableContext(null, null, DATABASE_NAME, SCHEMA_NAME,
            GEOMETRY_COLUMNS_TABLE_NAME, GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_COLUMN_PROTECTION_FLAGS,
            PROTECTED_DATABASE_NAMES, PROTECTED_SCHEMA_NAMES, PROTECTED_GEOMETRY_COLUMNS_TABLE_NAMES,
            PROTECTED_GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_WHERE_CLAUSE);

    @ClassRule
    public static RuleChain getRuleChain() {
        ProxyResource proxyResource = new ProxyResource(SECURITY_POLICY, TARGETS);
        return getRuleChain(proxyResource, BOREHOLES_SCRIPT, BOREHOLES_TABLE_NAME_IN_SCRIPT, SCHEMA_NAME,
                BOREHOLES_TABLE_NAME);
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
