package eu.clarussecure.proxy.pgsql;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;

public class BoreholesSearchableEncryptionIT extends BoreholesProtection {

    private static final String SECURITY_POLICY = "./src/test/resources/boreholes_3857_searchable_encryption.xml";

    private static final String BOREHOLES_TABLE_NAME = "tu_encrypted_boreholes_3857_se";
    private static final String[] BOREHOLES_COLUMN_NAMES = new String[] { "gid", "nom_com", "adresse", "code_bss",
            "denominati", "type_point", "district", "circonscri", "precision", "altitude", "prof_max", null, "geom" };

    private static final String PROTECTED_DATABASE_NAME = DATABASE_NAME;
    private static final String PROTECTED_SCHEMA_NAME = SCHEMA_NAME;
    private static final String PROTECTED_BOREHOLES_TABLE_NAME = BOREHOLES_TABLE_NAME;
    private static final String[] PROTECTED_BOREHOLES_COLUMN_NAMES = new String[] {
            // gid nom_com, adresse, code_bss, denominati, type_point, district,
            // circonscri, precision, altitude, prof_max, ?attribute1?, geom
            "gid", "?", "?", "?", "?", "?", "?", "?", "?", "?", "?", "rowID", "geom" };

    private static final String BOREHOLES_EXTRA_TABLE_NAME = "se_index";

    private static final String PROTECTED_GEOMETRY_COLUMNS_TABLE_NAME = GEOMETRY_COLUMNS_TABLE_NAME;
    private static final String[] PROTECTED_GEOMETRY_COLUMNS_COLUMN_NAMES = new String[] { "f_table_catalog",
            "f_table_schema", "f_table_name", "f_geometry_column", "coord_dimension", "srid", "type" };
    private static final boolean[] GEOMETRY_COLUMNS_COLUMN_PROTECTION_FLAGS = new boolean[] {
            // f_table_catalog, f_table_schema, f_table_name, f_geometry_column,
            // coord_dimension, srid, type
            false, false, false, false, false, false, false };
    private static final String GEOMETRY_COLUMNS_WHERE_CLAUSE = "f_table_name = '" + BOREHOLES_TABLE_NAME + "'";

    private static final String GEOMETRY_TYPE = "POINT";
    private static final String PROTECTED_GEOMETRY_TYPE = "POINT";

    private final DatasetContext boreholes = buildTableContext(1, BOREHOLES_SCRIPT, BOREHOLES_TABLE_NAME_IN_SCRIPT,
            DATABASE_NAME, SCHEMA_NAME, BOREHOLES_TABLE_NAME, BOREHOLES_COLUMN_NAMES, PROTECTED_DATABASE_NAME,
            PROTECTED_SCHEMA_NAME, PROTECTED_BOREHOLES_TABLE_NAME, PROTECTED_BOREHOLES_COLUMN_NAMES,
            BOREHOLES_WHERE_CLAUSE, BOREHOLES_EXTRA_TABLE_NAME);

    private final DatasetContext geometryColumns = buildTableContext(1, null, null, DATABASE_NAME, SCHEMA_NAME,
            GEOMETRY_COLUMNS_TABLE_NAME, GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_COLUMN_PROTECTION_FLAGS,
            PROTECTED_DATABASE_NAME, PROTECTED_SCHEMA_NAME, PROTECTED_GEOMETRY_COLUMNS_TABLE_NAME,
            PROTECTED_GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_WHERE_CLAUSE);

    @ClassRule
    public static RuleChain getRuleChain() {
        ProxyResource proxyResource = new ProxyResource(SECURITY_POLICY, TARGET);
        return getRuleChain(proxyResource, BOREHOLES_SCRIPT, BOREHOLES_TABLE_NAME_IN_SCRIPT, SCHEMA_NAME,
                BOREHOLES_TABLE_NAME, BOREHOLES_EXTRA_TABLE_NAME);
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
