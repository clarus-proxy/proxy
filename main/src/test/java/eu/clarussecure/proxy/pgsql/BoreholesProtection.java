package eu.clarussecure.proxy.pgsql;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import eu.clarussecure.proxy.spi.StringUtilities;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class BoreholesProtection extends DataSetProtection {

    protected static final String BOREHOLES_SCRIPT = "./src/test/resources/datasets/boreholes_3857_WKT.sql";
    protected static final String BOREHOLES_TABLE_NAME_IN_SCRIPT = "boreholes_3857";

    protected static final String[] BOREHOLES_COLUMN_NAMES = new String[] { "gid", "nom_com", "adresse", "code_bss",
            "denominati", "type_point", "district", "circonscri", "precision", "altitude", "prof_max", "geom" };
    protected static final String BOREHOLES_WHERE_CLAUSE = "gid = 2";

    protected static final String GEOMETRY_COLUMNS_TABLE_NAME = "geometry_columns";
    protected static final String[] GEOMETRY_COLUMNS_COLUMN_NAMES = new String[] { "f_table_catalog", "f_table_schema",
            "f_table_name", "f_geometry_column", "coord_dimension", "srid", "type" };

    protected abstract DatasetContext getBoreholes();

    protected abstract DatasetContext getGeometryColumns();

    protected abstract String getGeometryType();

    protected abstract String getProtectedGeometryType();

    @Test
    @DatasetResource.SkipInitialization
    public void a_first_createDataset() throws FileNotFoundException, IOException, SQLException {
        createDataset(getBoreholes());
    }

    @Test
    public void query_boreholes_1_selectAll_clearResults() throws SQLException {
        queryClearData(getBoreholes(), "*");
        queryClearData(getBoreholes(), "*", true);
    }

    @Test
    public void query_boreholes_1_selectAll_protectedResults() throws SQLException {
        queryProtectedData(getBoreholes(), "*");
        queryProtectedData(getBoreholes(), "*", true);
    }

    @Test
    public void query_boreholes_2_selectColumnsOneByOne_clearResults() throws SQLException {
        for (int c = 0; c < getBoreholes().getColumnNames().length; c++) {
            queryClearData(getBoreholes(), StringUtilities.quote(getBoreholes().getColumnNames()[c]));
            queryClearData(getBoreholes(), StringUtilities.quote(getBoreholes().getColumnNames()[c]), true);
        }
    }

    @Test
    public void query_boreholes_2_selectColumnsOneByOne_protectedResults() throws SQLException {
        for (int c = 0; c < getBoreholes().getColumnNames().length; c++) {
            queryProtectedData(getBoreholes(), StringUtilities.quote(getBoreholes().getColumnNames()[c]));
            queryProtectedData(getBoreholes(), StringUtilities.quote(getBoreholes().getColumnNames()[c]), true);
        }
    }

    @Test
    public void query_boreholes_3_selectDuplicatedColumns_clearResults() throws SQLException {
        queryClearData(getBoreholes(), "nom_com, adresse, st_astext(geom), *, gid");
        queryClearData(getBoreholes(), "nom_com, adresse, st_astext(geom), *, gid", true);
    }

    @Test
    public void query_boreholes_3_selectDuplicatedColumns_protectedResults() throws SQLException {
        queryProtectedData(getBoreholes(), "nom_com, adresse, st_astext(geom), *, gid");
        queryProtectedData(getBoreholes(), "nom_com, adresse, st_astext(geom), *, gid", true);
    }

    @Test
    public void query_boreholes_4_selectDuplicatedColumnsWithAliases_clearResults() throws SQLException {
        queryClearData(getBoreholes(), "nom_com as a, adresse as b, st_astext(geom) as c, *, gid as d");
        queryClearData(getBoreholes(), "nom_com as a, adresse as b, st_astext(geom) as c, *, gid as d", true);
    }

    @Test
    public void query_boreholes_4_selectDuplicatedColumnsWithAliases_protectedResults() throws SQLException {
        queryProtectedData(getBoreholes(), "nom_com as a, adresse as b, st_astext(geom) as c, *, gid as d");
        queryProtectedData(getBoreholes(), "nom_com as a, adresse as b, st_astext(geom) as c, *, gid as d", true);
    }

    @Test
    public void query_boreholes_5_selectWhereInEnvelope_clearResults() throws SQLException {
        queryClearData(getBoreholes(), "st_asbinary(geom,'NDR'), gid",
                "geom && st_makeenvelope(-20026376.39,-20048966.10,20026376.39,20048966.10,3857)", Integer.MAX_VALUE);
    }

    @Test
    public void query_boreholes_5_selectWhereInEnvelope_protectedResults() throws SQLException {
        queryProtectedData(getBoreholes(), "st_asbinary(geom,'NDR'), gid",
                "geom && st_makeenvelope(-20026376.39,-20048966.10,20026376.39,20048966.10,3857)", Integer.MAX_VALUE);
    }

    @Test
    public void query_boreholes_6_compareClearDataWithProtectedData() throws SQLException {
        compareClearDataWithProtectedData(getBoreholes(), (columnName, clearValue, protectedValue) -> {
            if (columnName.equals("geom")) {
                // TODO compare x and y
            }
        });
    }

    @Test
    public void query_boreholes_7_selectWhereDataEqual_clearResults() throws SQLException {
        List<List<String>> rows = queryClearData(getBoreholes(), "*", "nom_com = 'CAROMB'", Integer.MAX_VALUE, true);
        Assert.assertNotNull(rows);
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals(getBoreholes().getColumnNames().length, rows.get(0).size());
        Assert.assertEquals("CAROMB", rows.get(0).get(1));
    }

    @Test
    public void query_boreholes_7_selectWhereDataEqual_protectedResults() throws SQLException {
        List<List<String>> rows = queryProtectedData(getBoreholes(), "*", "nom_com = 'CAROMB'", Integer.MAX_VALUE,
                true);
        Assert.assertNotNull(rows);
        Assert.assertEquals(1, rows.size());
        int expectedNbColumns = (int) Arrays.stream(getBoreholes().getProtectedColumnNames())
                .flatMap(cspPcns -> Arrays.stream(cspPcns)).filter(pcn -> pcn != null).count();
        Assert.assertEquals(expectedNbColumns, rows.get(0).size());
        if (getBoreholes().isColumnProtected(1)) {
            Assert.assertNotEquals("CAROMB", rows.get(0).get(1));
        } else {
            Assert.assertEquals("CAROMB", rows.get(0).get(1));
        }
    }

    @Test
    public void query_geometry_columns_1_selectAll_clearResults() throws SQLException {
        queryClearData(getGeometryColumns(), "*");
        queryClearData(getGeometryColumns(), "*", true);
    }

    @Test
    public void query_geometry_columns_1_selectAll_protectedResults() throws SQLException {
        queryProtectedData(getGeometryColumns(), "*");
        queryProtectedData(getGeometryColumns(), "*", true);
    }

    @Test
    public void query_geometry_columns_2_selectColumnsOneByOne_clearResults() throws SQLException {
        for (int c = 0; c < getGeometryColumns().getColumnNames().length; c++) {
            queryClearData(getGeometryColumns(), StringUtilities.quote(getGeometryColumns().getColumnNames()[c]));
            queryClearData(getGeometryColumns(), StringUtilities.quote(getGeometryColumns().getColumnNames()[c]), true);
        }
    }

    @Test
    public void query_geometry_columns_2_selectColumnsOneByOne_protectedResults() throws SQLException {
        for (int c = 0; c < getGeometryColumns().getColumnNames().length; c++) {
            queryProtectedData(getGeometryColumns(), StringUtilities.quote(getGeometryColumns().getColumnNames()[c]));
            queryProtectedData(getGeometryColumns(), StringUtilities.quote(getGeometryColumns().getColumnNames()[c]),
                    true);
        }
    }

    @Test
    public void query_geometry_columns_3_selectDuplicatedColumns_clearResults() throws SQLException {
        queryClearData(getGeometryColumns(), "f_table_catalog, f_table_name, *, type");
        queryClearData(getGeometryColumns(), "f_table_catalog, f_table_name, *, type", true);
    }

    @Test
    public void query_geometry_columns_3_selectDuplicatedColumns_protectedResults() throws SQLException {
        queryProtectedData(getGeometryColumns(), "f_table_catalog, f_table_name, *, type");
        queryProtectedData(getGeometryColumns(), "f_table_catalog, f_table_name, *, type", true);
    }

    @Test
    public void query_geometry_columns_4_selectDuplicatedColumnsWithAliases_clearResults() throws SQLException {
        queryClearData(getGeometryColumns(), "f_table_catalog as a, f_table_name as b, *, type as c");
        queryClearData(getGeometryColumns(), "f_table_catalog as a, f_table_name as b, *, type as c", true);
    }

    @Test
    public void query_geometry_columns_4_selectDuplicatedColumnsWithAliases_protectedResults() throws SQLException {
        queryProtectedData(getGeometryColumns(), "f_table_catalog as a, f_table_name as b, *, type as c");
        queryProtectedData(getGeometryColumns(), "f_table_catalog as a, f_table_name as b, *, type as c", true);
    }

    @Test
    public void query_geometry_columns_5_compareClearDataWithProtectedData() throws SQLException {
        compareClearDataWithProtectedData(getGeometryColumns(), (columnName, clearValue, protectedValue) -> {
            if (columnName.equals("type")) {
                Assert.assertEquals(getGeometryType(), clearValue);
                Assert.assertEquals(getProtectedGeometryType(), protectedValue);
            }
        });
    }

    @Test
    @DatasetResource.SkipCleanup
    public void z_last_deleteDataset() throws SQLException {
        deleteDataset(getBoreholes());
    }
}
