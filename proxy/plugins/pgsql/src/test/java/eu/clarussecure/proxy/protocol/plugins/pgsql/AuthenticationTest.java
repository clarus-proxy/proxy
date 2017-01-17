package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

/**
 * <p>
 * Class to test authentication cases :
 * 
 * Case 1: Correct login and password.
 * Case 2: Correct login, wrong password.
 * Case 3: Non existing login, random password.
 * </p>
 * 
 * @author Mehdi.BENANIBA
 *
 */
public class AuthenticationTest {

    PgsqlProtocol pgsqlProtocol = null;

    @Before
    public void startProxy() throws Exception {
        pgsqlProtocol = new PgsqlProtocol();
        pgsqlProtocol.getConfiguration().setServerAddress(InetAddress.getByName("10.15.0.89"));
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

    @After
    public void stopProxy() {
        pgsqlProtocol.stop();
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * <p>
     * Case 1: Correct login and password.
     * User: postgres
     * Password: postgres
     * 
     * Shall connect without errors.
     * </p>
     * 
     * @throws SQLException
     */
    @Test
    public void testCase1NominalConnection() throws SQLException {
        String user = "postgres";
        String password = "postgres";
        try (Connection con = getHealthConnection(user, password)) {
            Assert.assertNotNull("Connection shall be opened.", con);
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * <p>
     * Case 2: Correct login, wrong password.
     * User: postgres
     * Password: random generated password
     * 
     * Shall fail because of the wrong password and throw PSQLException
     * with message containing "password authentication failed".
     * </p>
     * 
     * @throws SQLException
     */
    @Test
    public void testCase2WrongPasswordConnection() throws SQLException {
        String user = "postgres";
        String passwordRandom = TestUtils.generateRandomString();
        exception.expect(PSQLException.class);
        exception.expectMessage("password authentication failed for user");
        getHealthConnection(user, passwordRandom);
    }

    /**
     * <p>
     * Case 3: Non existing login.
     * User: random generated user name
     * Password: random generated password
     * 
     * Shall fail because of the wrong user and throw PSQLException
     * with message containing "no pg_hba.conf entry for host".
     * </p>
     * 
     * @throws SQLException
     */
    @Test
    public void testCase3WrongUserConnection() throws SQLException {
        String user = TestUtils.generateRandomString();
        String passwordRandom = TestUtils.generateRandomString();
        exception.expect(PSQLException.class);
        exception.expectMessage("no pg_hba.conf entry for host");
        getHealthConnection(user, passwordRandom);
    }

    /**
     * <p>
     * Connect to the PostgreSQL 'ehealth' database.
     * </p>
     * 
     * @return the PostgreSQL connection.
     * @throws SQLException
     */
    private Connection getHealthConnection(String user, String password) throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", user);
        connectionProps.put("password", password);
        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/ehealth", connectionProps);
    }

}
