package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

public class AuthenticationSSLToTwoBackendsTest {

    PgsqlProtocol pgsqlProtocol = null;

    public static enum SSLMode {
        DISABLED, ALLOWED, REQUIRED;
    }

    /**
     * <p>
     * Set USE_SELF_SIGNED_CERTIFICATE system variable to true (develop mode)
     * <p>
     */
    @BeforeClass
    public static void initializeSelfSignedCertificateTrue() {
        System.setProperty("tcp.ssl.use.self.signed.certificate", "true");
    }

    /**
     * <p>
     * Before each unit test, Proxy start
     * <p>
     * @throws Exception
     */
    @Before
    public void startProxy() throws Exception {
        pgsqlProtocol = new PgsqlProtocol();
        pgsqlProtocol.getConfiguration().setServerAddresses(InetAddress.getByName("10.15.0.89"),
                InetAddress.getByName("10.15.0.89"));
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.CREATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.READ, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.UPDATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.DELETE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.CREATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.READ, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.UPDATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.DELETE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().register(new ProtocolServiceNoop());
        pgsqlProtocol.start();
        Thread.sleep(500);
    }

    /**
     * <p>
     * After each unit test, Proxy stop
     * <p>
     */
    @After
    public void stopProxy() {
        pgsqlProtocol.stop();
    }

    /**
     * <p>
     * Specific rule which permit to attempt Exception to validate (or not) test
     * <p>
     */
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * <p>
     * Case Client SSL 1: Try a connection while both Client and Server allow SSL protocol
     * User: postgres
     * Password: postgres
     *
     * SSL connection shall be successful
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase1SslAllowedClientAndServer() throws Exception {
        Connection con = sslConnectionSslSettings("postgres", "postgres", "allowed", "allowed");
        Assert.assertNotNull("Connection shall be successful", con);
    }

    /**
     * <p>
     * Case Client SSL 2: Try a connection while Client allow SSL protocol and Server require SSL protocol
     * User: postgres
     * Password: postgres
     *
     * SSL connection shall be successful
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase2SslAllowedClientRequiredServer() throws Exception {
        Connection con = sslConnectionSslSettings("postgres", "postgres", "allowed", "required");
        Assert.assertNotNull("Connection shall be successful", con);
    }

    /**
     * <p>
     * Case Client SSL 3: Try a connection while Client allow SSL protocol and Server disable SSL protocol
     * User: postgres
     * Password: postgres
     *
     * SSL connection shall be successful (Proxy reply to Frontend for Server)
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase3SslAllowedClientDisabledServer() throws Exception {
        Connection con = sslConnectionSslSettings("postgres", "postgres", "allowed", "disabled");
        Assert.assertNotNull("Connection shall be successful", con);
    }

    /**
     * <p>
     * Case Client SSL 4: Try a connection while Client require SSL protocol and Server allow SSL protocol
     * User: postgres
     * Password: postgres
     *
     * SSL connection shall be successful
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase4SslRequiredClientAllowedServer() throws Exception {
        Connection con = sslConnectionSslSettings("postgres", "postgres", "required", "allowed");
        Assert.assertNotNull("Connection shall be successful", con);
    }

    /**
     * <p>
     * Case Client SSL 5: Try a connection while both Client & Server require SSL protocol
     * User: postgres
     * Password: postgres
     *
     * SSL connection shall be successful
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase5SslRequiredClientRequiredServer() throws Exception {
        Connection con = sslConnectionSslSettings("postgres", "postgres", "required", "required");
        Assert.assertNotNull("Connection shall be successful", con);
    }

    /**
     * <p>
     * Case Client SSL 6: Try a connection while Client require SSL protocol and Server disable SSL protocol
     * User: postgres
     * Password: postgres
     *
     * SSL connection shall be successful (Proxy reply to Frontend for Server)
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase6SslRequiredClientDisabledServer() throws Exception {
        Connection con = sslConnectionSslSettings("postgres", "postgres", "required", "disabled");
        Assert.assertNotNull("Connection shall be successful", con);
    }

    /**
     * <p>
     * Case Client SSL 7: Try a connection while Client disable SSL protocol and Server allow SSL protocol
     * User: postgres
     * Password: postgres
     *
     * Impossible to get a connection. Attempt shall throw PGSQL.Exception : Le serveur ne supporte pas SSL
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase7SslDisabledClientAllowedServer() throws Exception {
        exception.expect(PSQLException.class);
        exception.expectMessage("Le serveur ne supporte pas SSL");
        sslConnectionSslSettings("postgres", "postgres", "disabled", "allowed");
    }

    /**
     * <p>
     * Case Client SSL 8: Try a connection while Client disable SSL protocol and Server require SSL protocol
     * User: postgres
     * Password: postgres
     *
     * Impossible to get a connection. Attempt shall throw PGSQL.Exception : Le serveur ne supporte pas SSL
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase8SslDisabledClientRequiredServer() throws Exception {
        exception.expect(PSQLException.class);
        exception.expectMessage("Le serveur ne supporte pas SSL");
        sslConnectionSslSettings("postgres", "postgres", "disabled", "required");
    }

    /**
     * <p>
     * Case Client SSL 9: Try a connection while both Client & Server disable SSL protocol
     * User: postgres
     * Password: postgres
     *
     * Impossible to get a connection. Attempt shall throw PGSQL.Exception : Le serveur ne supporte pas SSL
     * </p>
     * @throws Exception
     */
    @Test
    public void testCase9SslDisabledClientDisabledServer() throws Exception {
        exception.expect(PSQLException.class);
        exception.expectMessage("Le serveur ne supporte pas SSL");
        sslConnectionSslSettings("postgres", "postgres", "disabled", "disabled");
    }

    /**
     * <p>
     * Case Client clear 1: Try a clear connection while both Client & Server allow SSL protocol
     *
     * Connection shall be successful
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase1ClearAllowedClientAllowedServer() throws SQLException {
        Connection con = clearConnectionSslSettings("postgres", "postgres", "allowed", "allowed");
        Assert.assertNotNull("Connection shall be successfull", con);
    }

    /**
     * <p>
     * Case Client clear 2: Try a clear connection while Client allow SSL protocol and Server require SSL protocol
     *
     * Connection shall be successful
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase2ClearAllowedClientRequiredServer() throws SQLException {
        Connection con = clearConnectionSslSettings("postgres", "postgres", "allowed", "required");
        Assert.assertNotNull("Connection shall be successfull", con);
    }

    /**
     * <p>
     * Case Client clear 3: Try a clear connection while Client allow SSL protocol and Server disable SSL protocol
     *
     * Connection shall be successful
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase3ClearAllowedClientDisabledServer() throws SQLException {
        Connection con = clearConnectionSslSettings("postgres", "postgres", "allowed", "disabled");
        Assert.assertNotNull("Connection shall be successfull", con);
    }

    /**
     * <p>
     * Case Client clear 4: Try a clear connection while Client require SSL protocol and Server allow SSL protocol
     *
     * Impossible to get a connection. Attempt shall throw PGSQL.Exception : SSL is required
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase4ClearRequiredClientAllowedServer() throws SQLException {
        exception.expect(PSQLException.class);
        exception.expectMessage("SSL is required");
        clearConnectionSslSettings("postgres", "postgres", "required", "allowed");
    }

    /**
     * <p>
     * Case Client clear 5: Try a clear connection while both Client & Server require SSL protocol
     *
     * Impossible to get a connection. Attempt shall throw PGSQL.Exception : SSL is required
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase5ClearRequiredClientRequiredServer() throws SQLException {
        exception.expect(PSQLException.class);
        exception.expectMessage("SSL is required");
        clearConnectionSslSettings("postgres", "postgres", "required", "required");
    }

    /**
     * <p>
     * Case Client clear 6: Try a clear connection while Client require SSL protocol and Server disable SSL protocol
     *
     * Impossible to get a connection. Attempt shall throw PGSQL.Exception : SSL is required
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase6ClearRequiredClientDisabledServer() throws SQLException {
        exception.expect(PSQLException.class);
        exception.expectMessage("SSL is required");
        clearConnectionSslSettings("postgres", "postgres", "required", "disabled");
    }

    /**
     * <p>
     * Case Client clear 7: Try a clear connection while Client disable SSL protocol and Server allow SSL protocol
     *
     * Connection shall be successful
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase7ClearDisabledClientAllowedServer() throws SQLException {
        Connection con = clearConnectionSslSettings("postgres", "postgres", "disabled", "allowed");
        Assert.assertNotNull("Connection shall be successfull", con);
    }

    /**
     * <p>
     * Case Client clear 8: Try a clear connection while Client disable SSL protocol and Server require SSL protocol
     *
     * Connection shall be successful
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase8ClearDisabledClientRequiredServer() throws SQLException {
        Connection con = clearConnectionSslSettings("postgres", "postgres", "disabled", "required");
        Assert.assertNotNull("Connection shall be successfull", con);
    }

    /**
     * <p>
     * Case Client clear 9: Try a clear connection while both Client & Server disable SSL protocol
     *
     * Connection shall be successful
     * </p>
     * @throws SQLException
     */
    @Test
    public void testCase9ClearDisabledClientDisabledServer() throws SQLException {
        Connection con = clearConnectionSslSettings("postgres", "postgres", "disabled", "disabled");
        Assert.assertNotNull("Connection shall be successfull", con);
    }

    /**
     * <p>
     * Connect to the PostgreSQL 'ehealth' database via SSL protocol.
     * Set SSL mode to client and server to check if these parameters could interfere with SSL connection
     * </p>
     * @param user
     * @param password
     * @param clientMode
     * @param serverMode
     * @throws SQLException
     */
    private Connection sslConnectionSslSettings(String user, String password, String clientMode, String serverMode)
            throws SQLException {
        Connection con = null;
        switch (clientMode) {
        case "allowed":
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
            break;
        case "required":
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.REQUIRED);
            break;
        case "disabled":
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.DISABLED);
            break;
        }
        switch (serverMode) {
        case "allowed":
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
            break;
        case "required":
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.REQUIRED);
            break;
        case "disabled":
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.DISABLED);
            break;
        }
        try {
            con = getHealthConnectionSSL(user, password);
            return con;
        } finally {
            TestUtils.closeConnection(con);
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
        }
    }

    /**
     * <p>
     * Connect to the PostgreSQL 'ehealth' database.
     * Set SSL mode to client and server to check if these parameters could interfere with clear connection
     * </p>
     * @param user
     * @param password
     * @param clientMode
     * @param serverMode
     * @throws SQLException
     */
    private Connection clearConnectionSslSettings(String user, String password, String clientMode, String serverMode)
            throws SQLException {
        Connection con = null;
        switch (clientMode) {
        case "allowed":
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
            break;
        case "required":
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.REQUIRED);
            break;
        case "disabled":
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.DISABLED);
            break;
        }
        switch (serverMode) {
        case "allowed":
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
            break;
        case "required":
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.REQUIRED);
            break;
        case "disabled":
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.DISABLED);
            break;
        }
        try {
            con = getHealthConnection(user, password);
            return con;
        } finally {
            TestUtils.closeConnection(con);
            SSLSessionInitializer.setClientMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
            SSLSessionInitializer.setServerMode(
                    eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode.ALLOWED);
        }
    }

    /**
     * <p>
     * Connect to the PostgreSQL 'ehealth' database.
     * </p>
     *
     * @param user
     * @param password
     * @return
     * @throws SQLException
     */
    private Connection getHealthConnection(String user, String password) throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("user", user);
        connectionProps.setProperty("password", password);
        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/ehealth", connectionProps);
    }

    /**
     * <p>
     * Connect to the PostgreSQL 'ehealth' database via SSL protocol.
     * </p>
     *
     * @param user
     * @param password
     * @return Postgres Connection
     * @throws SQLException
     */
    private Connection getHealthConnectionSSL(String user, String password) throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("user", user);
        connectionProps.setProperty("password", password);
        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/ehealth?" + "ssl=true&"
                + "sslfactory=org.postgresql.ssl.NonValidatingFactory", connectionProps);
    }

}
