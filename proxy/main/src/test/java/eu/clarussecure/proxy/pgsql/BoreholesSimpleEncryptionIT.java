package eu.clarussecure.proxy.pgsql;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;

public class BoreholesSimpleEncryptionIT extends BoreholesProtection {

    private static final String SECURITY_POLICY = "./src/test/resources/boreholes_3857_encryption.xml";
    private static final String TARGET = "10.15.0.89";

    private static final String PROTECTED_DATABASE_NAME = DATABASE_NAME;
    private static final String PROTECTED_SCHEMA_NAME = SCHEMA_NAME;
    private static final String PROTECTED_BOREHOLES_TABLE_NAME = BOREHOLES_TABLE_NAME;
    private static final String[] PROTECTED_BOREHOLES_COLUMN_NAMES = new String[] {
            // gid nom_com, adresse, code_bss, denominati, type_point, district,
            // circonscri, precision, altitude, prof_max, geom
            "gid", "nom_com", "adresse", "code_bss", "denominati", "type_point", "district", "circonscri", "precision",
            "altitude", "prof_max", "?" };

    private static final String PROTECTED_GEOMETRY_COLUMNS_TABLE_NAME = GEOMETRY_COLUMNS_TABLE_NAME;
    private static final String[] PROTECTED_GEOMETRY_COLUMNS_COLUMN_NAMES = new String[] { "f_table_catalog",
            "f_table_schema", "f_table_name", "f_geometry_column", "coord_dimension", "srid", "type" };
    private static final boolean[] GEOMETRY_COLUMNS_COLUMN_PROTECTION_FLAGS = new boolean[] {
            // f_table_catalog, f_table_schema, f_table_name, f_geometry_column,
            // coord_dimension, srid, type
            false, false, false, true, false, false, false };

    private static final String GEOMETRY_TYPE = "POINT";
    private static final String PROTECTED_GEOMETRY_TYPE = "POINT";

    private final DatasetContext boreholes = buildTableContext(1, BOREHOLES_SCRIPT, DATABASE_NAME, SCHEMA_NAME,
            BOREHOLES_TABLE_NAME, BOREHOLES_COLUMN_NAMES, PROTECTED_DATABASE_NAME, PROTECTED_SCHEMA_NAME,
            PROTECTED_BOREHOLES_TABLE_NAME, PROTECTED_BOREHOLES_COLUMN_NAMES, BOREHOLES_WHERE_CLAUSE);

    private final DatasetContext geometryColumns = buildTableContext(1, null, DATABASE_NAME, SCHEMA_NAME,
            GEOMETRY_COLUMNS_TABLE_NAME, GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_COLUMN_PROTECTION_FLAGS,
            PROTECTED_DATABASE_NAME, PROTECTED_SCHEMA_NAME, PROTECTED_GEOMETRY_COLUMNS_TABLE_NAME,
            PROTECTED_GEOMETRY_COLUMNS_COLUMN_NAMES, GEOMETRY_COLUMNS_WHERE_CLAUSE);

    @ClassRule
    public static RuleChain getRuleChain() {
        MongoDBServerResource mongoDBServerResource = new MongoDBServerResource("localhost", 27017);
        ProxyResource proxyResource = new ProxyResource(SECURITY_POLICY, TARGET);
        ConnectionResource connectionResource = new ConnectionResource();
        DatasetResource datasetResource = new DatasetResource(BOREHOLES_SCRIPT, SCHEMA_NAME, BOREHOLES_TABLE_NAME,
                connectionResource);
        return RuleChain.outerRule(mongoDBServerResource).around(proxyResource).around(connectionResource)
                .around(datasetResource);
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
