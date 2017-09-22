package eu.clarussecure.proxy.pgsql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.util.PSQLState;

import eu.clarussecure.proxy.Proxy;
import eu.clarussecure.proxy.protection.mongodb.EmbeddedMongoDB;
import eu.clarussecure.proxy.spi.StringUtilities;

@RunWith(Parameterized.class)
public abstract class DataSetProtection {

    protected class DatasetContext {
        private final String script;
        private final String tableNameInScript;
        private final String tableName;
        private final String[] columnNames;
        private final String[][] protectedColumnNames;
        private final boolean[] columnProtectionFlags;
        private final String whereClause;
        private final String extraTableName;

        public DatasetContext(String script, String tableNameInScript, String tableName, String[] columnNames,
                String[][] protectedColumnNames, boolean[] columnProtectedFlags, String whereClause,
                String extraTableName) {
            this.script = script;
            this.tableNameInScript = tableNameInScript;
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.protectedColumnNames = protectedColumnNames;
            this.columnProtectionFlags = columnProtectedFlags;
            this.whereClause = whereClause;
            this.extraTableName = extraTableName;
        }

        public String getScript() {
            return script;
        }

        public String getTableNameInScript() {
            return tableNameInScript;
        }

        public String getTableName() {
            return tableName;
        }

        public String[] getColumnNames() {
            return getColumnNames(false);
        }

        public String[] getColumnNames(boolean all) {
            if (all) {
                return columnNames;
            } else {
                return Arrays.stream(columnNames).filter(cn -> cn != null).toArray(String[]::new);
            }
        }

        public String[][] getProtectedColumnNames() {
            return protectedColumnNames;
        }

        public int getNumberOfCSPs() {
            return getProtectedColumnNames().length;
        }

        public boolean[] getColumnProtectionFlags() {
            return columnProtectionFlags;
        }

        public boolean isColumnProtected(int column) {
            return getColumnProtectionFlags()[column];
        }

        public String getWhereClause() {
            return whereClause;
        }

        public String getExtraTableName() {
            return extraTableName;
        }

        private String toColumnName(String selectItem, boolean resolveRealColumnName) {
            String columnName = null;
            int idx = selectItem == null ? -1 : selectItem.indexOf(" as ");
            if (idx != -1) {
                if (resolveRealColumnName) {
                    selectItem = selectItem.substring(0, idx).trim();
                } else {
                    columnName = selectItem.substring(idx + " as ".length()).trim();
                }
            }
            if (columnName == null) {
                idx = selectItem == null ? -1 : selectItem.indexOf('(');
                if (idx != -1) {
                    if (resolveRealColumnName) {
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
                        columnName = selectItem.substring(0, idx).trim();
                    }
                }
            }
            if (columnName == null) {
                columnName = selectItem;
            }
            return columnName;
        }

        private String toColumnName(String selectItem, int involvedCSP) {
            if (involvedCSP != -1) {
                String columnName = selectItem == null ? null : toColumnName(selectItem, true);
                int idx = Arrays.asList(getColumnNames(true)).indexOf(columnName);
                if (idx != -1) {
                    columnName = involvedCSP < getProtectedColumnNames().length
                            ? getProtectedColumnNames()[involvedCSP][idx] : null;
                }
                return columnName;
            } else {
                return toColumnName(selectItem, false);
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
            boolean anyAsteriskColumn = Arrays.stream(selectItemTokens)
                    .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
                    .anyMatch(selectItem -> selectItem.equals("*"));
            boolean anyProtectedColumn = Arrays.stream(selectItemTokens)
                    .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
                    .map(selectItem -> StringUtilities.unquote(selectItem)).anyMatch(selectItem -> {
                        if (selectItem.equals("*")) {
                            return IntStream.range(0, getColumnProtectionFlags().length)
                                    .anyMatch(i -> getColumnProtectionFlags()[i]);
                        } else {
                            int index = Arrays.asList(getColumnNames(true)).indexOf(selectItem);
                            return index != -1 && isColumnProtected(index);
                        }
                    });
            String[] expectedClearColumnNames = Arrays.stream(involvedCSPs)
                    .mapToObj(
                            csp -> Arrays.stream(selectItemTokens)
                                    .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
                                    .map(selectItem -> StringUtilities.unquote(selectItem))
                                    .map(selectItem -> selectItem
                                            .equals("*") ? Arrays.stream(getColumnNames(csp != -1))
                                                    : Stream.of(selectItem))
                                    .flatMap(stream -> (!anyAsteriskColumn && anyProtectedColumn)
                                            ? Stream.concat(stream,
                                                    Arrays.stream(getColumnNames(csp != -1)).filter(cn -> cn == null))
                                            : stream)
                                    .map(selectItem -> toColumnName(selectItem, csp != -1)))
                    .flatMap(stream -> stream).toArray(String[]::new);
            String[] expectedColumnNames = Arrays.stream(involvedCSPs)
                    .mapToObj(
                            csp -> Arrays.stream(selectItemTokens)
                                    .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
                                    .map(selectItem -> StringUtilities.unquote(selectItem))
                                    .map(selectItem -> selectItem
                                            .equals("*") ? Arrays.stream(getColumnNames(csp != -1))
                                                    : Stream.of(selectItem))
                                    .flatMap(stream -> (!anyAsteriskColumn && anyProtectedColumn)
                                            ? Stream.concat(stream,
                                                    Arrays.stream(getColumnNames(csp != -1)).filter(cn -> cn == null))
                                            : stream)
                                    .map(selectItem -> toColumnName(selectItem, csp)).filter(cn -> cn != null))
                    .flatMap(stream -> stream).toArray(String[]::new);
            String query = "SELECT " + selectItems + " FROM " + getTableName();
            if (whereClause != null) {
                query = query + " WHERE " + whereClause;
            }
            List<List<String>> result = null;
            try (Statement statement = connectionResource.getConnection().createStatement()) {
                int nbRows = 0;
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    Assert.assertNotNull(resultSet);
                    Assert.assertNotNull(resultSet.getMetaData());
                    Assert.assertEquals(expectedColumnNames.length, resultSet.getMetaData().getColumnCount());
                    for (int c = 0; c < resultSet.getMetaData().getColumnCount(); c++) {
                        String expectedClearColumnName = expectedClearColumnNames[c];
                        String expectedColumnName = expectedColumnNames[c];
                        String actualColumnName = resultSet.getMetaData().getColumnLabel(c + 1);
                        if (expectedColumnName.endsWith("?")) {
                            int index = actualColumnName.lastIndexOf('.');
                            index = actualColumnName.lastIndexOf('.', index - 1) + 1;
                            String actualShortColumnName = actualColumnName.substring(index);
                            if (expectedClearColumnName != null) {
                                Assert.assertNotEquals(getTableName() + "." + expectedClearColumnName,
                                        actualShortColumnName);
                            }
                            Pattern pattern = Pattern
                                    .compile(expectedColumnName.replace(".", "\\.").replace("?", ".*"));
                            Assert.assertTrue(pattern.matcher(actualColumnName).matches());
                        } else {
                            Assert.assertEquals(expectedColumnName, actualColumnName);
                        }
                        // bug with splitting: need to manage table names
                        if (getNumberOfCSPs() == 1) {
                            String actualTableName = resultSet.getMetaData().getTableName(c + 1);
                            if (!actualTableName.isEmpty()) {
                                String expectedClearTableName = getTableName();
                                String expectedTableName;
                                if (protectedNames) {
                                    int end = expectedColumnName.lastIndexOf('.');
                                    int begin = expectedColumnName.lastIndexOf('.', end - 1) + 1;
                                    expectedTableName = expectedColumnName.substring(begin, end);
                                } else {
                                    expectedTableName = expectedClearTableName;
                                }
                                if (expectedTableName.equals("?")) {
                                    Assert.assertNotEquals(expectedClearTableName, actualTableName);
                                } else {
                                    Assert.assertEquals(expectedTableName, actualTableName);
                                }
                            }
                        }
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
    protected static final String TARGET;
    protected static final String[] TARGETS;

    static {
        String target = System.getProperty("TARGET");
        String targets = System.getProperty("TARGETS");
        TARGET = target != null ? target : targets.split(";")[0];
        TARGETS = targets != null ? targets.split(";") : new String[] { target };
    }

    protected DatasetContext buildTableContext(int nbCSPs, String script, String tableNameInScript, String databaseName,
            String schemaName, String tableName, String[] columnNames, String whereClause) {
        String protectedDatabaseName = databaseName;
        String protectedSchemaName = schemaName;
        String protectedTableName = tableName;
        String[] protectedColumnNames = columnNames;
        return buildTableContext(nbCSPs, script, tableNameInScript, databaseName, schemaName, tableName, columnNames,
                protectedDatabaseName, protectedSchemaName, protectedTableName, protectedColumnNames, whereClause);
    }

    protected DatasetContext buildTableContext(int nbCSPs, String script, String tableNameInScript, String databaseName,
            String schemaName, String tableName, String[] columnNames, String protectedDatabaseName,
            String protectedSchemaName, String protectedTableName, String[] protectedColumnNames, String whereClause) {
        return buildTableContext(nbCSPs, script, tableNameInScript, databaseName, schemaName, tableName, columnNames,
                protectedDatabaseName, protectedSchemaName, protectedTableName, protectedColumnNames, whereClause,
                null);
    }

    protected DatasetContext buildTableContext(int nbCSPs, String script, String tableNameInScript, String databaseName,
            String schemaName, String tableName, String[] columnNames, String protectedDatabaseName,
            String protectedSchemaName, String protectedTableName, String[] protectedColumnNames, String whereClause,
            String extraTableName) {
        boolean[] columnProtectionFlags = new boolean[columnNames.length];
        IntStream.range(0, columnNames.length).forEach(c -> columnProtectionFlags[c] = c < protectedColumnNames.length
                ? !Objects.equals(columnNames[c], protectedColumnNames[c]) : true);
        return buildTableContext(nbCSPs, script, tableNameInScript, databaseName, schemaName, tableName, columnNames,
                columnProtectionFlags, protectedDatabaseName, protectedSchemaName, protectedTableName,
                protectedColumnNames, whereClause, extraTableName);
    }

    protected DatasetContext buildTableContext(int nbCSPs, String script, String tableNameInScript, String databaseName,
            String schemaName, String tableName, String[] columnNames, boolean[] columnProtectionFlags,
            String protectedDatabaseName, String protectedSchemaName, String protectedTableName,
            String[] protectedColumnNames, String whereClause) {
        return buildTableContext(nbCSPs, script, tableNameInScript, databaseName, schemaName, tableName, columnNames,
                columnProtectionFlags, protectedDatabaseName, protectedSchemaName, protectedTableName,
                protectedColumnNames, whereClause, null);
    }

    protected DatasetContext buildTableContext(int nbCSPs, String script, String tableNameInScript, String databaseName,
            String schemaName, String tableName, String[] columnNames, boolean[] columnProtectionFlags,
            String protectedDatabaseName, String protectedSchemaName, String protectedTableName,
            String[] protectedColumnNames, String whereClause, String extraTableName) {
        String[] protectedDatabaseNames = IntStream.range(0, nbCSPs).mapToObj(csp -> protectedDatabaseName)
                .toArray(String[]::new);
        String[] protectedSchemaNames = IntStream.range(0, nbCSPs).mapToObj(csp -> protectedSchemaName)
                .toArray(String[]::new);
        String[] protectedTableNames = IntStream.range(0, nbCSPs).mapToObj(csp -> protectedTableName)
                .toArray(String[]::new);
        String[][] allProtectedColumnNames = IntStream.range(0, nbCSPs).mapToObj(csp -> IntStream
                .range(0, protectedColumnNames.length).mapToObj(c -> protectedColumnNames[c]).toArray(String[]::new))
                .toArray(String[][]::new);
        return buildTableContext(script, tableNameInScript, databaseName, schemaName, tableName, columnNames,
                columnProtectionFlags, protectedDatabaseNames, protectedSchemaNames, protectedTableNames,
                allProtectedColumnNames, whereClause, extraTableName);
    }

    protected DatasetContext buildTableContext(String script, String tableNameInScript, String databaseName,
            String schemaName, String tableName, String[] columnNames, boolean[] columnProtectionFlags,
            String[] protectedDatabaseNames, String[] protectedSchemaNames, String[] protectedTableNames,
            String[][] protectedColumnNames, String whereClause) {
        return buildTableContext(script, tableNameInScript, databaseName, schemaName, tableName, columnNames,
                columnProtectionFlags, protectedDatabaseNames, protectedSchemaNames, protectedTableNames,
                protectedColumnNames, whereClause, null);
    }

    protected DatasetContext buildTableContext(String script, String tableNameInScript, String databaseName,
            String schemaName, String tableName, String[] columnNames, boolean[] columnProtectionFlags,
            String[] protectedDatabaseNames, String[] protectedSchemaNames, String[] protectedTableNames,
            String[][] protectedColumnNames, String whereClause, String extraTableName) {
        String[][] fqProtectedColumnNames = IntStream.range(0, protectedColumnNames.length)
                .mapToObj(csp -> IntStream.range(0, protectedColumnNames[csp].length)
                        .mapToObj(c -> protectedColumnNames[csp][c] != null ? "csp" + (csp + 1) + "."
                                + protectedDatabaseNames[csp] + "." + protectedSchemaNames[csp] + "."
                                + protectedTableNames[csp] + "." + protectedColumnNames[csp][c] : null)
                        .toArray(String[]::new))
                .toArray(String[][]::new);
        return new DatasetContext(script, tableNameInScript, tableName, columnNames, fqProtectedColumnNames,
                columnProtectionFlags, whereClause, extraTableName);
    }

    protected static class ProxyResource extends ExternalResource {
        private final String securityPolicy;
        private final String[] targets;
        private Proxy proxy;

        public ProxyResource(String securityPolicy, String... targets) {
            this.securityPolicy = securityPolicy;
            this.targets = targets;
        }

        @Override
        protected void before() throws Throwable {
            System.setProperty("pgsql.sql.force.processing", "true");
            String[] arguments = Stream.concat(Stream.of("--security-policy", securityPolicy), Stream.of(targets))
                    .toArray(String[]::new);
            proxy = Proxy.builder(arguments);
            if (proxy != null) {
                proxy.initialize();
                proxy.start();
                proxy.waitForServerIsReady();
            }
        }

        @Override
        protected void after() {
            proxy.stop();
            try {
                proxy.sync();
            } catch (InterruptedException | ExecutionException e) {
                // should not occur
                e.printStackTrace();
            }
        }
    };

    protected static class MongoDBServerResource extends ExternalResource {
        private final EmbeddedMongoDB server;

        public MongoDBServerResource(String host, int port) {
            server = new EmbeddedMongoDB(host, port);
        }

        @Override
        protected void before() throws Throwable {
            server.start();
        }

        @Override
        protected void after() {
            server.stop();
        }
    };

    protected static class ConnectionResource extends ExternalResource {
        public interface Configuration {
            String getPreferQueryMode();
        }

        private final Configuration configuration;
        private Connection connection;

        public ConnectionResource() {
            this(null);
        }

        public ConnectionResource(Configuration configurable) {
            this.configuration = configurable;
        }

        @Override
        protected void before() throws Throwable {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new SQLException(e);
            }
            Properties connectionProps = new Properties();
            connectionProps.put("user", "postgres");
            connectionProps.put("password", "postgres");
            if (configuration != null) {
                connectionProps.put("preferQueryMode", configuration.getPreferQueryMode());
            }
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + DATABASE_NAME,
                    connectionProps);
        }

        @Override
        protected void after() {
            try {
                connection.close();
            } catch (SQLException e) {
                // should not occur
                e.printStackTrace();
            }
        }

        public String getPreferQueryMode() {
            return configuration != null ? configuration.getPreferQueryMode() : null;
        }

        public Connection getConnection() {
            return connection;
        }
    };

    protected static class DatasetResource extends ExternalResource {

        @Retention(RetentionPolicy.RUNTIME)
        @Target({ ElementType.METHOD })
        public @interface SkipInitialization {
        }

        @Retention(RetentionPolicy.RUNTIME)
        @Target({ ElementType.METHOD })
        public @interface SkipCleanup {
        }

        private final String script;
        private final String tableNameInScript;
        private final String schemaName;
        private final String tableName;
        private final String extraTableName;
        private final ConnectionResource connectionResource;
        private boolean skipInitialization = false;
        private boolean skipCleanup = false;

        public DatasetResource(String script, String tableNameInScript, String schemaName, String tableName,
                ConnectionResource connectionResource) {
            this(script, tableNameInScript, schemaName, tableName, null, connectionResource);
        }

        public DatasetResource(String script, String tableNameInScript, String schemaName, String tableName,
                String extraTableName, ConnectionResource connectionResource) {
            this.script = script;
            this.tableNameInScript = tableNameInScript;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.extraTableName = extraTableName;
            this.connectionResource = connectionResource;
        }

        @Override
        public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement base,
                Description description) {
            skipInitialization = hasToSkip(description, SkipInitialization.class);
            skipCleanup = hasToSkip(description, SkipCleanup.class);
            return super.apply(base, description);
        }

        private <T extends Annotation> boolean hasToSkip(Description description, Class<T> annotationType) {
            if (description.isTest()) {
                if (description.getAnnotation(annotationType) != null) {
                    return true;
                }
            } else {
                for (Description childDescription : description.getChildren()) {
                    if (hasToSkip(childDescription, annotationType)) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        protected void before() throws Throwable {
            if (!skipInitialization) {
                removeTablesIfExist();
                try (FileInputStream fin = new FileInputStream(script);
                        ReplacingInputStream rin = new ReplacingInputStream(fin,
                                tableNameInScript.getBytes(StandardCharsets.UTF_8),
                                tableName.getBytes(StandardCharsets.UTF_8));
                        InputStreamReader reader = new InputStreamReader(rin, StandardCharsets.UTF_8)) {
                    connectionResource.getConnection().setAutoCommit(true);
                    ScriptRunner scriptRunner = new ScriptRunner(connectionResource.getConnection());
                    scriptRunner.setSendFullScript("simple".equals(connectionResource.getPreferQueryMode()));
                    scriptRunner.setAutoCommit(true);
                    scriptRunner.runScript(reader);
                }
            }
        }

        private void removeTablesIfExist() throws SQLException {
            try (Statement statement = connectionResource.getConnection().createStatement()) {
                statement.executeUpdate(String.format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
                if (extraTableName != null) {
                    statement.executeUpdate(String.format("DROP TABLE IF EXISTS %s.%s", schemaName, extraTableName));
                }
            }
        }

        @Override
        protected void after() {
            if (!skipCleanup) {
                try (Statement statement = connectionResource.getConnection().createStatement()) {
                    statement.executeUpdate(String.format("DROP TABLE %s.%s", schemaName, tableName));
                    if (extraTableName != null) {
                        statement
                                .executeUpdate(String.format("DROP TABLE IF EXISTS %s.%s", schemaName, extraTableName));
                    }
                } catch (SQLException e) {
                    // should not occur
                    e.printStackTrace();
                }
            }
        }
    };

    protected static RuleChain getRuleChain(ProxyResource proxyResource, String script, String tableNameInScript,
            String schemaName, String tableName) {
        return getRuleChain(proxyResource, script, tableNameInScript, schemaName, tableName, null);
    }

    protected static RuleChain getRuleChain(ProxyResource proxyResource, String script, String tableNameInScript,
            String schemaName, String tableName, String extraTableName) {
        ConnectionResource connectionResource = new ConnectionResource();
        DatasetResource datasetResource = new DatasetResource(script, tableNameInScript, schemaName, tableName,
                extraTableName, connectionResource);
        return RuleChain.outerRule(proxyResource).around(connectionResource).around(datasetResource);
    }

    @Parameters(name = "{0}_queries")
    public static Iterable<? extends Object> getPreferQueryMode() {
        return Arrays.asList("simple", "extended");
    }

    @Parameter
    public String preferQueryMode;

    @Rule
    public ConnectionResource connectionResource = new ConnectionResource(new ConnectionResource.Configuration() {

        @Override
        public String getPreferQueryMode() {
            return preferQueryMode;
        }
    });

    protected List<List<String>> query(DatasetContext tableContext, String selectItems, String whereClause,
            int expectedNbRows, boolean returnResult) throws SQLException {
        return tableContext.executeQuery(selectItems, whereClause, expectedNbRows, returnResult);
    }

    protected void query(DatasetContext tableContext, String selectItems, String whereClause, int expectedNbRows)
            throws SQLException {
        query(tableContext, selectItems, whereClause, expectedNbRows, false);
    }

    protected List<List<String>> queryClearData(DatasetContext tableContext, String selectItems, String whereClause,
            int expectedNbRows, boolean returnResult) throws SQLException {
        return query(tableContext, selectItems, whereClause, expectedNbRows, returnResult);
    }

    protected void queryClearData(DatasetContext tableContext, String selectItems, String whereClause,
            int expectedNbRows) throws SQLException {
        queryClearData(tableContext, selectItems, whereClause, expectedNbRows, false);
    }

    protected void queryClearData(DatasetContext tableContext, String selectItems, boolean withWhereClause)
            throws SQLException {
        queryClearData(tableContext, selectItems, withWhereClause ? tableContext.getWhereClause() : null,
                withWhereClause ? 1 : Integer.MAX_VALUE);
    }

    protected void queryClearData(DatasetContext tableContext, String selectItems) throws SQLException {
        queryClearData(tableContext, selectItems, false);
    }

    protected List<List<String>> queryProtectedData(DatasetContext tableContext, String selectItems, String whereClause,
            int expectedNbRows) throws SQLException {
        return queryProtectedData(tableContext, selectItems, whereClause, expectedNbRows, false);
    }

    protected List<List<String>> queryProtectedData(DatasetContext tableContext, String selectItems, String whereClause,
            int expectedNbRows, boolean returnResult) throws SQLException {
        String[] selectItemTokens = selectItems.split(",(?![^(]*\\))");
        String[] expectedColumnNames = Arrays.stream(selectItemTokens).map(String::trim)
                .flatMap(selectItem -> selectItem.equals("*") ? Arrays.stream(tableContext.getColumnNames())
                        : Stream.of(selectItem))
                .map(selectItem -> StringUtilities.unquote(selectItem)).toArray(String[]::new);
        // protected data by CSP
        for (int csp = 1; csp <= tableContext.getNumberOfCSPs(); csp++) {
            String protectedSelectItems = String.format("clarus_protected('csp%d'), %s", csp, selectItems);
            final int csp2 = csp - 1;
            boolean expectedResults = Arrays.stream(expectedColumnNames).map(cn -> tableContext.toColumnName(cn, csp2))
                    .filter(cn -> cn != null).count() > 0L;
            query(tableContext, protectedSelectItems, whereClause, expectedResults ? expectedNbRows : 0);
        }
        // protected data for a CSP that is not involved
        String protectedSelectItems = String.format("clarus_protected('csp%d'), %s", tableContext.getNumberOfCSPs() + 1,
                selectItems);
        query(tableContext, protectedSelectItems, whereClause, 0);
        // protected data for all CSPs (plus one that is not involved)
        StringBuilder sb = new StringBuilder("clarus_protected(");
        boolean expectedResults = false;
        for (int csp = 1; csp <= tableContext.getNumberOfCSPs(); csp++) {
            sb.append(String.format("'csp%d', ", csp));
            final int csp2 = csp - 1;
            expectedResults |= Arrays.stream(expectedColumnNames).map(cn -> tableContext.toColumnName(cn, csp2))
                    .filter(cn -> cn != null).count() > 0L;
        }
        sb.append(String.format("'csp%d'), %s", tableContext.getNumberOfCSPs() + 1, selectItems));
        protectedSelectItems = sb.toString();
        return query(tableContext, protectedSelectItems, whereClause, expectedResults ? expectedNbRows : 0,
                returnResult);
    }

    protected void queryProtectedData(DatasetContext tableContext, String selectItems, boolean withWhereClause)
            throws SQLException {
        queryProtectedData(tableContext, selectItems, withWhereClause ? tableContext.getWhereClause() : null,
                withWhereClause ? 1 : Integer.MAX_VALUE);
    }

    protected void queryProtectedData(DatasetContext tableContext, String selectItems) throws SQLException {
        queryProtectedData(tableContext, selectItems, false);
    }

    @FunctionalInterface
    protected interface TestColumnValues {
        void test(String columnName, String clearValue, String protectedValue);
    }

    protected void compareClearDataWithProtectedData(DatasetContext tableContext, TestColumnValues testColumnValues)
            throws SQLException {
        int additionalColumns = (int) Arrays.stream(tableContext.getColumnNames(true)).filter(cn -> cn == null).count();
        for (int c = 0; c < tableContext.getColumnNames(true).length; c++) {
            if (tableContext.getColumnNames(true)[c] == null) {
                continue;
            }
            boolean countAdditionalColumns = tableContext.isColumnProtected(c);
            List<List<String>> clearResult = tableContext
                    .executeQuery(StringUtilities.quote(tableContext.getColumnNames(true)[c]), true, 1, true);
            Assert.assertNotNull(clearResult);
            Assert.assertEquals(clearResult.size(), 1);
            Assert.assertNotNull(clearResult.get(0));
            Assert.assertEquals(clearResult.get(0).size(), 1);
            List<List<List<String>>> protectedResults = new ArrayList<>(tableContext.getNumberOfCSPs());
            for (int csp = 0; csp < tableContext.getNumberOfCSPs(); csp++) {
                boolean expectedResults = tableContext.toColumnName(tableContext.getColumnNames(true)[c], csp) != null;
                List<List<String>> protectedResult = tableContext.executeQuery(
                        String.format("clarus_protected('csp%d'), %s", (csp + 1),
                                StringUtilities.quote(tableContext.getColumnNames(true)[c])),
                        true, expectedResults ? 1 : 0, true);
                if (expectedResults) {
                    Assert.assertNotNull(protectedResult);
                    Assert.assertEquals(1, protectedResult.size());
                    Assert.assertNotNull(protectedResult.get(0));
                    Assert.assertEquals(1 + (countAdditionalColumns ? additionalColumns : 0),
                            protectedResult.get(0).size());
                } else {
                    Assert.assertNull(protectedResult);
                }
                protectedResults.add(protectedResult);
            }
            for (int csp = 0; csp < tableContext.getNumberOfCSPs(); csp++) {
                List<List<String>> protectedResult = protectedResults.get(csp);
                boolean expectedResults = tableContext.toColumnName(tableContext.getColumnNames(true)[c], csp) != null;
                if (expectedResults) {
                    if (tableContext.isColumnProtected(c)) {
                        Assert.assertNotEquals(clearResult.get(0).get(0), protectedResult.get(0).get(0));
                    } else {
                        Assert.assertEquals(clearResult.get(0).get(0), protectedResult.get(0).get(0));
                    }
                    if (testColumnValues != null) {
                        testColumnValues.test(tableContext.getColumnNames(true)[c], clearResult.get(0).get(0),
                                protectedResult.get(0).get(0));
                    }
                } else {
                    Assert.assertNull(protectedResult);
                }
            }
        }
    }

    protected void createDataset(DatasetContext tableContext) throws FileNotFoundException, IOException, SQLException {
        String script = tableContext.getScript();
        try (FileInputStream fin = new FileInputStream(script);
                ReplacingInputStream rin = new ReplacingInputStream(fin,
                        tableContext.getTableNameInScript().getBytes(StandardCharsets.UTF_8),
                        tableContext.getTableName().getBytes(StandardCharsets.UTF_8));
                InputStreamReader reader = new InputStreamReader(rin, StandardCharsets.UTF_8)) {
            connectionResource.getConnection().setAutoCommit(true);
            ScriptRunner scriptRunner = new ScriptRunner(connectionResource.getConnection());
            scriptRunner.setLogWriter(null);
            scriptRunner.setSendFullScript("simple".equals(connectionResource.getPreferQueryMode()));
            scriptRunner.setAutoCommit(true);
            scriptRunner.runScript(reader);
        }
    }

    protected void deleteDataset(DatasetContext tableContext) throws SQLException {
        try (Statement statement = connectionResource.getConnection().createStatement()) {
            statement.executeUpdate(String.format("DROP TABLE %s", tableContext.getTableName()));
            if (tableContext.getExtraTableName() != null) {
                statement.executeUpdate(String.format("DROP TABLE %s", tableContext.getExtraTableName()));
            }
        }
    }

}
