package eu.clarussecure.proxy.pgsql;

import java.io.BufferedReader;
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

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.ExtractedArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.store.HttpProxyFactory;
import de.flapdoodle.embed.process.extract.ITempNaming;
import de.flapdoodle.embed.process.extract.UUIDTempNaming;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.runtime.Network;
import eu.clarussecure.proxy.Proxy;
import eu.clarussecure.proxy.spi.StringUtilities;

@RunWith(Parameterized.class)
public abstract class DataSetProtection {

    protected class DatasetContext {
        private final String script;
        private final String tableName;
        private final String[] columnNames;
        private final String[][] protectedColumnNames;
        private final boolean[] columnProtectionFlags;
        private final String whereClause;

        public DatasetContext(String script, String tableName, String[] columnNames, String[][] protectedColumnNames,
                boolean[] columnProtectedFlags, String whereClause) {
            this.script = script;
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.protectedColumnNames = protectedColumnNames;
            this.columnProtectionFlags = columnProtectedFlags;
            this.whereClause = whereClause;
        }

        public String getScript() {
            return script;
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

        public boolean[] getColumnProtectionFlags() {
            return columnProtectionFlags;
        }

        public boolean isColumnProtected(int column) {
            return getColumnProtectionFlags()[column];
        }

        public String getWhereClause() {
            return whereClause;
        }

        private String toColumnName(String selectItem, boolean resolveRealColumnName) {
            String columnName = null;
            int idx = selectItem.indexOf(" as ");
            if (idx != -1) {
                if (resolveRealColumnName) {
                    selectItem = selectItem.substring(0, idx).trim();
                } else {
                    columnName = selectItem.substring(idx + " as ".length()).trim();
                }
            }
            if (columnName == null) {
                idx = selectItem.indexOf('(');
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
                String columnName = toColumnName(selectItem, true);
                int idx = Arrays.asList(getColumnNames()).indexOf(columnName);
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
            String[] expectedClearColumnNames = Arrays.stream(involvedCSPs)
                    .mapToObj(csp -> Arrays.stream(selectItemTokens)
                            .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
                            .flatMap(selectItem -> selectItem.equals("*") ? Arrays.stream(getColumnNames())
                                    : Stream.of(selectItem))
                            .map(selectItem -> StringUtilities.unquote(selectItem))
                            .map(selectItem -> toColumnName(selectItem, csp != -1)).filter(cn -> cn != null))
                    .flatMap(stream -> stream).toArray(String[]::new);
            String[] expectedColumnNames = Arrays.stream(involvedCSPs)
                    .mapToObj(csp -> Arrays.stream(selectItemTokens)
                            .filter(selectItem -> !selectItem.startsWith("clarus_protected(")).map(String::trim)
                            .flatMap(selectItem -> selectItem.equals("*") ? Arrays.stream(getColumnNames())
                                    : Stream.of(selectItem))
                            .map(selectItem -> StringUtilities.unquote(selectItem))
                            .map(selectItem -> toColumnName(selectItem, csp)).filter(cn -> cn != null))
                    .flatMap(stream -> stream).toArray(String[]::new);
            String query = "select " + selectItems + " from " + getTableName();
            if (whereClause != null) {
                query = query + " where " + whereClause;
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
                        if (expectedColumnName.contains("?")) {
                            int index = actualColumnName.lastIndexOf('.');
                            index = actualColumnName.lastIndexOf('.', index - 1) + 1;
                            String actualShortColumnName = actualColumnName.substring(index);
                            Assert.assertNotEquals(getTableName() + "." + expectedClearColumnName,
                                    actualShortColumnName);
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

    protected DatasetContext buildTableContext(int nbCSPs, String script, String databaseName, String schemaName,
            String tableName, String[] columnNames, String whereClause) {
        String protectedDatabaseName = databaseName;
        String protectedSchemaName = schemaName;
        String protectedTableName = tableName;
        String[] protectedColumnNames = columnNames;
        return buildTableContext(nbCSPs, script, databaseName, schemaName, tableName, columnNames,
                protectedDatabaseName, protectedSchemaName, protectedTableName, protectedColumnNames, whereClause);
    }

    protected DatasetContext buildTableContext(int nbCSPs, String script, String databaseName, String schemaName,
            String tableName, String[] columnNames, String protectedDatabaseName, String protectedSchemaName,
            String protectedTableName, String[] protectedColumnNames, String whereClause) {
        String[] protectedDatabaseNames = IntStream.range(0, nbCSPs).mapToObj(csp -> protectedDatabaseName)
                .toArray(String[]::new);
        String[] protectedSchemaNames = IntStream.range(0, nbCSPs).mapToObj(csp -> protectedSchemaName)
                .toArray(String[]::new);
        String[] protectedTableNames = IntStream.range(0, nbCSPs).mapToObj(csp -> protectedTableName)
                .toArray(String[]::new);
        String[][] allProtectedColumnNames = IntStream.range(0, nbCSPs).mapToObj(csp -> IntStream
                .range(0, columnNames.length).mapToObj(c -> protectedColumnNames[c]).toArray(String[]::new))
                .toArray(String[][]::new);
        boolean[] columnProtectionFlags = new boolean[columnNames.length];
        Arrays.fill(columnProtectionFlags, false);
        return buildTableContext(script, databaseName, schemaName, tableName, columnNames, columnProtectionFlags,
                protectedDatabaseNames, protectedSchemaNames, protectedTableNames, allProtectedColumnNames,
                whereClause);
    }

    protected DatasetContext buildTableContext(String script, String databaseName, String schemaName, String tableName,
            String[] columnNames, boolean[] columnProtectionFlags, String[] protectedDatabaseNames,
            String[] protectedSchemaNames, String[] protectedTableNames, String[][] protectedColumnNames,
            String whereClause) {
        String[][] fqProtectedColumnNames = IntStream.range(0, protectedColumnNames.length)
                .mapToObj(csp -> IntStream.range(0, protectedColumnNames[csp].length)
                        .mapToObj(c -> protectedColumnNames[csp][c] != null ? "csp" + (csp + 1) + "."
                                + protectedDatabaseNames[csp] + "." + protectedSchemaNames[csp] + "."
                                + protectedTableNames[csp] + "." + protectedColumnNames[csp][c] : null)
                        .toArray(String[]::new))
                .toArray(String[][]::new);
        return new DatasetContext(script, tableName, columnNames, fqProtectedColumnNames, columnProtectionFlags,
                whereClause);
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
        private final String host;
        private final int port;
        private MongodExecutable mongodExecutable;
        private MongodProcess mongodProcess;

        public MongoDBServerResource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        protected void before() throws Throwable {
            IRuntimeConfig runtimeConfig = null;
            if (runtimeConfig == null) {
                String artifactStorePath = System.getProperty("mongodb.artifact.store.path");
                if (artifactStorePath != null) {
                    IDirectory artifactStoreDirectoryPath = new FixedPath(artifactStorePath);
                    ITempNaming executableNaming = new UUIDTempNaming();
                    Command command = Command.MongoD;

                    runtimeConfig = new RuntimeConfigBuilder().defaults(command)
                            .artifactStore(new ExtractedArtifactStoreBuilder().defaults(command)
                                    .download(new DownloadConfigBuilder().defaultsForCommand(command)
                                            .artifactStorePath(artifactStoreDirectoryPath).build())
                                    .executableNaming(executableNaming))
                            .build();
                }
            }
            if (runtimeConfig == null) {
                String hostName = System.getProperty("http.proxyHost", System.getProperty("https.proxyHost"));
                if (hostName != null) {
                    int port = Integer
                            .parseInt(System.getProperty("http.proxyPort", System.getProperty("https.proxyPort")));
                    Command command = Command.MongoD;
                    runtimeConfig = new RuntimeConfigBuilder().defaults(command)
                            .artifactStore(
                                    new ExtractedArtifactStoreBuilder().defaults(command)
                                            .download(new DownloadConfigBuilder().defaultsForCommand(command)
                                                    .proxyFactory(new HttpProxyFactory(hostName, port)).build()))
                            .build();
                }
            }
            if (runtimeConfig == null) {
                runtimeConfig = new RuntimeConfigBuilder().defaults(Command.MongoD).build();
            }
            MongodStarter starter = MongodStarter.getInstance(runtimeConfig);

            mongodExecutable = starter.prepare(new MongodConfigBuilder().version(Version.Main.PRODUCTION)
                    .net(new Net(host, port, Network.localhostIsIPv6())).build());
            mongodProcess = mongodExecutable.start();
        }

        @Override
        protected void after() {
            mongodProcess.stop();
            mongodExecutable.stop();
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
        private final String schemaName;
        private final String tableName;
        private final ConnectionResource connectionResource;
        private boolean skipInitialization = false;
        private boolean skipCleanup = false;

        public DatasetResource(String script, String schemaName, String tableName,
                ConnectionResource connectionResource) {
            this.script = script;
            this.schemaName = schemaName;
            this.tableName = tableName;
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
                boolean exist = doesTableExist();
                if (!exist) {
                    try (FileInputStream in = new FileInputStream(script);
                            InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
                            BufferedReader bufferedReader = new BufferedReader(reader)) {
                        connectionResource.getConnection().setAutoCommit(true);
                        ScriptRunner scriptRunner = new ScriptRunner(connectionResource.getConnection());
                        scriptRunner.setSendFullScript("simple".equals(connectionResource.getPreferQueryMode()));
                        scriptRunner.setAutoCommit(true);
                        scriptRunner.runScript(bufferedReader);
                    }
                } else {
                    skipCleanup = true;
                }
            }
        }

        private boolean doesTableExist() throws SQLException {
            boolean exist = false;
            try (Statement statement = connectionResource.getConnection().createStatement();
                    ResultSet resultSet = statement.executeQuery(String.format(
                            "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s');",
                            schemaName, tableName))) {
                Assert.assertNotNull(resultSet);
                if (resultSet.next()) {
                    exist = resultSet.getBoolean(1);
                }
            }
            return exist;
        }

        @Override
        protected void after() {
            if (!skipCleanup) {
                try (Statement statement = connectionResource.getConnection().createStatement()) {
                    statement.executeUpdate("drop table " + tableName);
                } catch (SQLException e) {
                    // should not occur
                    e.printStackTrace();
                }
            }
        }
    };

    protected static RuleChain getRuleChain(ProxyResource proxyResource, String script, String schemaName,
            String tableName) {
        ConnectionResource connectionResource = new ConnectionResource();
        DatasetResource datasetResource = new DatasetResource(script, schemaName, tableName, connectionResource);
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

    protected void queryClearData(DatasetContext tableContext, String selectItems, String whereClause,
            int expectedNbRows) throws SQLException {
        query(tableContext, selectItems, whereClause, expectedNbRows);
    }

    protected void queryClearData(DatasetContext tableContext, String selectItems, boolean withWhereClause)
            throws SQLException {
        queryClearData(tableContext, selectItems, withWhereClause ? tableContext.getWhereClause() : null,
                withWhereClause ? 1 : Integer.MAX_VALUE);
    }

    protected void queryClearData(DatasetContext tableContext, String selectItems) throws SQLException {
        queryClearData(tableContext, selectItems, false);
    }

    protected void queryProtectedData(DatasetContext tableContext, String selectItems, String whereClause,
            int expectedNbRows) throws SQLException {
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
        query(tableContext, protectedSelectItems, whereClause, expectedResults ? expectedNbRows : 0);
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
        for (int c = 0; c < tableContext.getColumnNames().length; c++) {
            List<List<String>> clearResult = tableContext
                    .executeQuery(StringUtilities.quote(tableContext.getColumnNames()[c]), true, 1, true);
            Assert.assertNotNull(clearResult);
            Assert.assertEquals(clearResult.size(), 1);
            Assert.assertNotNull(clearResult.get(0));
            Assert.assertEquals(clearResult.get(0).size(), 1);
            List<List<List<String>>> protectedResults = new ArrayList<>(tableContext.getNumberOfCSPs());
            for (int csp = 0; csp < tableContext.getNumberOfCSPs(); csp++) {
                boolean expectedResults = tableContext.toColumnName(tableContext.getColumnNames()[c], csp) != null;
                List<List<String>> protectedResult = tableContext.executeQuery(
                        String.format("clarus_protected('csp%d'), %s", (csp + 1),
                                StringUtilities.quote(tableContext.getColumnNames()[c])),
                        true, expectedResults ? 1 : 0, true);
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
                    if (tableContext.isColumnProtected(c)) {
                        Assert.assertNotEquals(clearResult.get(0).get(0), protectedResult.get(0).get(0));
                    } else {
                        Assert.assertEquals(clearResult.get(0).get(0), protectedResult.get(0).get(0));
                    }
                    if (testColumnValues != null) {
                        testColumnValues.test(tableContext.getColumnNames()[c], clearResult.get(0).get(0),
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
        try (FileInputStream in = new FileInputStream(script);
                InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {
            connectionResource.getConnection().setAutoCommit(true);
            ScriptRunner scriptRunner = new ScriptRunner(connectionResource.getConnection());
            scriptRunner.setLogWriter(null);
            scriptRunner.setSendFullScript("simple".equals(connectionResource.getPreferQueryMode()));
            scriptRunner.setAutoCommit(true);
            scriptRunner.runScript(bufferedReader);
        }
    }

    protected void deleteDataset(DatasetContext tableContext) throws SQLException {
        try (Statement statement = connectionResource.getConnection().createStatement()) {
            statement.executeUpdate("drop table " + tableContext.getTableName());
        }
    }

}
