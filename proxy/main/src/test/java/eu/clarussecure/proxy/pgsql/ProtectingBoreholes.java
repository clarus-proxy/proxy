package eu.clarussecure.proxy.pgsql;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.postgresql.util.PSQLState;

import eu.clarussecure.proxy.Proxy;
import eu.clarussecure.proxy.spi.StringUtilities;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class ProtectingBoreholes {

    protected static class TableContext {
        private final String tableName;
        private final String[] columnNames;
        private final String[][] protectedColumnNames;
        private final boolean[] differentValueFlags;
        private final String whereClause;

        public static TableContext build(int nbCSPs, String databaseName, String schemaName, String tableName,
                String[] columnNames, String whereClause) {
            boolean[][] protectedColumnsPerCSP = new boolean[nbCSPs][columnNames.length];
            Arrays.stream(protectedColumnsPerCSP).forEach(cspColumns -> Arrays.fill(cspColumns, true));
            return build(databaseName, schemaName, tableName, columnNames, protectedColumnsPerCSP, whereClause);
        }

        public static TableContext build(String databaseName, String schemaName, String tableName, String[] columnNames,
                boolean[][] protectedColumnsPerCSP, String whereClause) {
            boolean[] differentValueFlags = new boolean[columnNames.length];
            Arrays.fill(differentValueFlags, false);
            return build(databaseName, schemaName, tableName, columnNames, protectedColumnsPerCSP, differentValueFlags,
                    whereClause);
        }

        public static TableContext build(String databaseName, String schemaName, String tableName, String[] columnNames,
                boolean[][] protectedColumnsPerCSP, boolean[] differentValueFlags, String whereClause) {
            String[] protectedDatabaseNames = new String[] { databaseName };
            String[] protectedSchemaNames = new String[] { schemaName };
            return build(protectedDatabaseNames, protectedSchemaNames, tableName, columnNames, protectedColumnsPerCSP, differentValueFlags, whereClause);
        }

        public static TableContext build(String[] protectedDatabaseNames, String[] protectedSchemaNames, String tableName, String[] columnNames,
                boolean[][] protectedColumnsPerCSP, boolean[] differentValueFlags, String whereClause) {
            String[][] protectedColumnNames = IntStream.range(0, protectedColumnsPerCSP.length)
                    .mapToObj(csp -> IntStream.range(0, columnNames.length)
                            .mapToObj(c -> protectedColumnsPerCSP[csp][c] ? "csp" + (csp + 1) + "." + protectedDatabaseNames[csp] + "."
                                    + protectedSchemaNames[csp] + "." + tableName + "." + columnNames[c] : null)
                            .toArray(String[]::new))
                    .toArray(String[][]::new);
            return new TableContext(tableName, columnNames, protectedColumnNames, differentValueFlags, whereClause);
        }

        public TableContext(String tableName, String[] columnNames,
                String[][] protectedColumnNames, boolean[] differentValueFlags, String whereClause) {
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.protectedColumnNames = protectedColumnNames;
            this.differentValueFlags = differentValueFlags;
            this.whereClause = whereClause;
        }

        public String getTableName() {
            return tableName;
        }

        public String[] getColumnNames() {
            return columnNames;
        }

        public String[][] getProtectedColumnNames() {
            return protectedColumnNames;
        }

        public int getNumberOfCSPs() {
            return getProtectedColumnNames().length;
        }

        public boolean[] getDifferentValueFlags() {
            return differentValueFlags;
        }

        public String getWhereClause() {
            return whereClause;
        }

        // bug with splitting: need to manage table names
//        private String toTableName(String selectItem) {
//            int idx = selectItem.indexOf(" as ");
//            if (idx != -1) {
//                return toTableName(selectItem.substring(0, idx).trim());
//            }
//            idx = selectItem.indexOf('(');
//            if (idx != -1) {
//                return "";
//            }
//            idx = selectItem.indexOf('.');
//            if (idx != -1) {
//                return selectItem.substring(0, idx).trim();
//            }
//            return getTableName();
//        }

        private String toColumnName(String selectItem, int involvedCSP) {
            if (involvedCSP != -1) {
                String columnName = selectItem;
                int idx = selectItem.indexOf('(');
                if (idx != -1) {
                    int idx2 = selectItem.indexOf(')', idx + 1);
                    String params = selectItem.substring(idx + 1, idx2);
                    String[] paramTokens = params.split(",");
                    for (String token : paramTokens) {
                        token = StringUtilities.unquote(token.trim());
                        idx = Arrays.asList(getColumnNames()).indexOf(token);
                        if (idx != -1) {
                            columnName = token;
                            break;
                        }
                    }
                } else {
                    idx = selectItem.indexOf(" as ");
                    if (idx != -1) {
                        columnName = selectItem.substring(0, idx).trim();
                    }
                }
                idx = Arrays.asList(getColumnNames()).indexOf(columnName);
                if (idx != -1) {
                    columnName = involvedCSP < getProtectedColumnNames().length ? getProtectedColumnNames()[involvedCSP][idx] : null;
                }
                return columnName;
            } else {
                String columnName = selectItem;
                int idx = selectItem.indexOf(" as ");
                if (idx != -1) {
                    columnName = selectItem.substring(idx + " as ".length()).trim();
                } else {
                    idx = selectItem.indexOf('(');
                    if (idx != -1) {
                        columnName = selectItem.substring(0, idx).trim();
                    }
                }
                return columnName;
            }
        }

        public void executeQuery(String selectItems, boolean withWhereClause, int expectedNbRows) throws SQLException {
            executeQuery(selectItems, withWhereClause ? getWhereClause() : null, expectedNbRows);
        }

        public void executeQuery(String selectItems, String whereClause, int expectedNbRows) throws SQLException {
            executeQuery(selectItems, whereClause, expectedNbRows, false);
        }

        public List<List<String>> executeQuery(String selectItems, boolean withWhereClause, int expectedNbRows,
                boolean returnResult) throws SQLException {
            return executeQuery(selectItems, withWhereClause ? getWhereClause() : null, expectedNbRows, returnResult);
        }

        public List<List<String>> executeQuery(String selectItems, String whereClause, int expectedNbRows,
                boolean returnResult) throws SQLException {
            String[] selectItemTokens = selectItems.split(",(?![^(]*\\))");
            boolean protectedNames = Arrays.stream(selectItemTokens)
                    .anyMatch(selectItem -> selectItem.startsWith("clarus_protected("));
            int[] involvedCSPs = protectedNames ? Arrays.stream(selectItemTokens)
                    .filter(selectItem -> selectItem.startsWith("clarus_protected(")).map(String::trim)
                    .map(s -> s.substring(s.indexOf('(') + 1, s.indexOf(')'))).map(s -> s.split(","))
                    .flatMap(Arrays::stream).map(String::trim).map(StringUtilities::unquote)
                    .map(s -> s.substring("csp".length())).mapToInt(Integer::parseInt).map(csp -> csp - 1).toArray()
                    : new int[] { -1 };
            String[] expectedColumnNames = Arrays.stream(involvedCSPs)
                    .mapToObj(csp -> Arrays.stream(selectItemTokens)
                            .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
                            .flatMap(selectItem -> selectItem.equals("*") ? Arrays.stream(getColumnNames())
                                    : Stream.of(selectItem))
                            .map(selectItem -> StringUtilities.unquote(selectItem))
                            .map(selectItem -> toColumnName(selectItem, csp)).filter(cn -> cn != null))
                    .flatMap(stream -> stream).toArray(String[]::new);
            // bug with splitting: need to manage table names
//            String[] expectedTableNames = Arrays.stream(selectItemTokens)
//                    .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
//                    .flatMap(selectItem -> selectItem.equals("*") ? Arrays.stream(getColumnNames())
//                            : Stream.of(selectItem))
//                    .map(selectItem -> StringUtilities.unquote(selectItem)).map(selectItem -> toTableName(selectItem))
//                    .toArray(String[]::new);
            String query = "select " + selectItems + " from " + getTableName();
            if (whereClause != null) {
                query = query + " where " + whereClause;
            }
            List<List<String>> result = null;
            try (Connection connection = connectToProxy(); Statement statement = connection.createStatement()) {
                int nbRows = 0;
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    Assert.assertNotNull(resultSet);
                    Assert.assertNotNull(resultSet.getMetaData());
                    Assert.assertEquals(expectedColumnNames.length, resultSet.getMetaData().getColumnCount());
                    for (int c = 0; c < resultSet.getMetaData().getColumnCount(); c++) {
                        String expectedColumnName = expectedColumnNames[c];
                        String actualColumnName = resultSet.getMetaData().getColumnLabel(c + 1);
                        Assert.assertEquals(expectedColumnName, actualColumnName);
                        // bug with splitting: need to manage table names
//                        String expectedTableName = expectedTableNames[c];
//                        String actualTableName = resultSet.getMetaData().getTableName(c + 1);
//                        Assert.assertEquals(expectedTableName, actualTableName);
                    }
                    if (returnResult) {
                        result = new ArrayList<>();
                    }
                    while (resultSet.next()) {
                        if (returnResult) {
                            List<String> row = new ArrayList<>(resultSet.getMetaData().getColumnCount());
                            for (int c = 1; c <= resultSet.getMetaData().getColumnCount(); c++) {
                                row.add(resultSet.getString(c));
                            }
                            result.add(row);
                        }
                        ++nbRows;
                    }
                } catch (SQLException e) {
                    Assert.assertEquals(0, nbRows);
                    Assert.assertEquals(PSQLState.NO_DATA.getState(), e.getSQLState());
                }
                if (expectedNbRows == Integer.MAX_VALUE) {
                    Assert.assertTrue(nbRows > 0);
                } else {
                    Assert.assertEquals(expectedNbRows, nbRows);
                }
            }
            return result;
        }

    }

    protected static final String DATABASE_NAME = "postgres";
    protected static final String SCHEMA_NAME = "public";
    protected static final String BOREHOLES_TABLE_NAME = "coarsen_boreholes_3857bis";
    protected static final String[] BOREHOLES_COLUMN_NAMES = new String[] { "gid", "nom_com", "adresse", "code_bss",
            "denominati", "type_point", "district", "circonscri", "precision", "altitude", "prof_max", "geom" };
    protected static final String BOREHOLES_WHERE_CLAUSE = "gid = 2";
    protected static final String GEOMETRY_COLUMNS_TABLE_NAME = "geometry_columns";
    protected static final String[] GEOMETRY_COLUMNS_COLUMN_NAMES = new String[] { "f_table_catalog", "f_table_schema",
            "f_table_name", "f_geometry_column", "coord_dimension", "srid", "type" };
    protected static final String GEOMETRY_COLUMNS_WHERE_CLAUSE = "f_table_name = '" + BOREHOLES_TABLE_NAME + "'";

    protected static Proxy startProxy(String securityPolicy, String... targets) throws Exception {
        System.setProperty("pgsql.sql.force.processing", "true");
        String[] arguments = Stream.concat(Stream.of("--security-policy", securityPolicy), Stream.of(targets))
                .toArray(String[]::new);
        Proxy proxy = Proxy.builder(arguments);
        if (proxy != null) {
            proxy.initialize();
            proxy.start();
            proxy.waitForServerIsReady();
        }
        return proxy;
    }

    protected static void stopProxy(Proxy proxy) throws Exception {
        proxy.stop();
    }

    protected static Connection connectToProxy() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        Properties connectionProps = new Properties();
        connectionProps.put("user", "postgres");
        connectionProps.put("password", "postgres");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + DATABASE_NAME,
                connectionProps);
        return connection;
    }

    @Test
    public void a_first_createDataset() throws FileNotFoundException, IOException, SQLException {
        String script = "./src/test/resources/datasets/boreholes_3857bis_WKT.sql";
        try (FileReader fileReader = new FileReader(script);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                Connection connection = connectToProxy()) {
            connection.setAutoCommit(true);
            ScriptRunner scriptRunner = new ScriptRunner(connection);
            scriptRunner.setAutoCommit(true);
            scriptRunner.runScript(bufferedReader);
        }
    }

    protected abstract TableContext getBoreholes();

    protected abstract TableContext getGeometryColumns();

    protected abstract String getGeometryType();

    protected abstract String getProtectedGeometryType();

    private List<List<String>> query(TableContext tableContext, String selectItems, String whereClause, int expectedNbRows, boolean returnResult) throws SQLException {
        return tableContext.executeQuery(selectItems, whereClause, expectedNbRows, returnResult);
    }

    private void query(TableContext tableContext, String selectItems, String whereClause, int expectedNbRows) throws SQLException {
        query(tableContext, selectItems, whereClause, expectedNbRows, false);
    }

    private void queryClearData(TableContext tableContext, String selectItems, String whereClause, int expectedNbRows) throws SQLException {
        query(tableContext, selectItems, whereClause, expectedNbRows);
    }

    private void queryClearData(TableContext tableContext, String selectItems, boolean withWhereClause) throws SQLException {
        queryClearData(tableContext, selectItems, withWhereClause ? tableContext.getWhereClause() : null,
                withWhereClause ? 1 : Integer.MAX_VALUE);
    }

    private void queryClearData(TableContext tableContext, String selectItems) throws SQLException {
        queryClearData(tableContext, selectItems, false);
    }

    private void queryProtectedData(TableContext tableContext, String selectItems, String whereClause, int expectedNbRows) throws SQLException {
        String[] selectItemTokens = selectItems.split(",(?![^(]*\\))");
        String[] expectedColumnNames = Arrays.stream(selectItemTokens)
                        .map(String::trim)
                        .flatMap(selectItem -> selectItem.equals("*") ? Arrays.stream(tableContext.getColumnNames())
                                : Stream.of(selectItem))
                        .map(selectItem -> StringUtilities.unquote(selectItem)).toArray(String[]::new);
        // protected data by CSP
        for (int csp = 1; csp <= tableContext.getNumberOfCSPs(); csp ++) {
            String protectedSelectItems = String.format("clarus_protected('csp%d'), %s", csp, selectItems);
            final int csp2 = csp - 1;
            boolean expectedResults = Arrays.stream(expectedColumnNames).map(cn -> tableContext.toColumnName(cn, csp2))
                    .filter(cn -> cn != null).count() > 0L;
            query(tableContext, protectedSelectItems, whereClause, expectedResults ? expectedNbRows : 0);
        }
        // protected data for a CSP that is not involved
        String protectedSelectItems = String.format("clarus_protected('csp%d'), %s", tableContext.getNumberOfCSPs() + 1, selectItems);
        query(tableContext, protectedSelectItems, whereClause, 0);
        // protected data for all  CSPs (plus one that is not involved)
        StringBuilder sb = new StringBuilder("clarus_protected(");
        boolean expectedResults = false;
        for (int csp = 1; csp <= tableContext.getNumberOfCSPs(); csp ++) {
            sb.append(String.format("'csp%d', ", csp));
            final int csp2 = csp - 1;
            expectedResults |= Arrays.stream(expectedColumnNames).map(cn -> tableContext.toColumnName(cn, csp2))
                    .filter(cn -> cn != null).count() > 0L;
        }
        sb.append(String.format("'csp%d'), %s", tableContext.getNumberOfCSPs() + 1, selectItems));
        protectedSelectItems = sb.toString();
        query(tableContext, protectedSelectItems, whereClause, expectedResults ? expectedNbRows : 0);
    }

    private void queryProtectedData(TableContext tableContext, String selectItems, boolean withWhereClause) throws SQLException {
        queryProtectedData(tableContext, selectItems, withWhereClause ? tableContext.getWhereClause() : null,
                withWhereClause ? 1 : Integer.MAX_VALUE);
    }

    private void queryProtectedData(TableContext tableContext, String selectItems) throws SQLException {
        queryProtectedData(tableContext, selectItems, false);
    }

    @FunctionalInterface
    private interface TestColumnValues {
        void test(String columnName, String clearValue, String protectedValue);
    }

    private void compareClearDataWithProtectedData(TableContext tableContext, TestColumnValues testColumnValues) throws SQLException {
        for (int c = 0; c < tableContext.getColumnNames().length; c++) {
            List<List<String>> clearResult = tableContext.executeQuery(StringUtilities.quote(tableContext.getColumnNames()[c]), true, 1, true);
            Assert.assertNotNull(clearResult);
            Assert.assertEquals(clearResult.size(), 1);
            Assert.assertNotNull(clearResult.get(0));
            Assert.assertEquals(clearResult.get(0).size(), 1);
            List<List<List<String>>> protectedResults = new ArrayList<>(tableContext.getNumberOfCSPs());
            for (int csp = 0; csp < tableContext.getNumberOfCSPs(); csp++) {
                boolean expectedResults = tableContext.toColumnName(tableContext.getColumnNames()[c], csp) != null;
                List<List<String>> protectedResult = tableContext.executeQuery(
                        String.format("clarus_protected('csp%d'), %s", (csp + 1), StringUtilities.quote(tableContext.getColumnNames()[c])), true, expectedResults ? 1 : 0, true);
                if (expectedResults) {
                    Assert.assertNotNull(protectedResult);
                    Assert.assertEquals(protectedResult.size(), 1);
                    Assert.assertNotNull(protectedResult.get(0));
                    Assert.assertEquals(protectedResult.get(0).size(), 1);
                } else {
                    Assert.assertNull(protectedResult);
                }
                protectedResults.add(protectedResult);
            }
            for (int csp = 0; csp < tableContext.getNumberOfCSPs(); csp++) {
                List<List<String>> protectedResult = protectedResults.get(csp);
                boolean expectedResults = tableContext.toColumnName(tableContext.getColumnNames()[c], csp) != null;
                if (expectedResults) {
                    if (tableContext.getDifferentValueFlags()[c]) {
                        Assert.assertNotEquals(clearResult.get(0).get(0), protectedResult.get(0).get(0));
                    } else {
                        Assert.assertEquals(clearResult.get(0).get(0), protectedResult.get(0).get(0));
                    }
                    if (testColumnValues != null) {
                        testColumnValues.test(tableContext.getColumnNames()[c], clearResult.get(0).get(0), protectedResult.get(0).get(0));
                    }
                } else {
                    Assert.assertNull(protectedResult);
                }
            }
        }
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
                "geom && st_makeenvelope(-20026376.39,-20048966.10,20026376.39,20048966.10,3857)",
                Integer.MAX_VALUE);
    }

    @Test
    public void query_boreholes_5_selectWhereInEnvelope_protectedResults() throws SQLException {
        queryProtectedData(getBoreholes(), "st_asbinary(geom,'NDR'), gid",
                "geom && st_makeenvelope(-20026376.39,-20048966.10,20026376.39,20048966.10,3857)",
                Integer.MAX_VALUE);
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
            queryProtectedData(getGeometryColumns(), StringUtilities.quote(getGeometryColumns().getColumnNames()[c]), true);
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
    public void z_last_deleteDataset() throws SQLException {
        try (Connection connection = connectToProxy(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop table " + getBoreholes().getTableName());
        }
    }
}
