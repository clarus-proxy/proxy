package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.tcp.ssl.SSLSessionInitializer.SSLMode;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

@RunWith(Theories.class)
public class Session2 {

    @DataPoints("nb_backends")
    public static final int[] NB_BACKENDS = { 1, 2 };

    @DataPoints("ssl_modes")
    public static final SSLMode[][] SSL_MODES = Stream.of(SSLMode.values())
            .flatMap(m1 -> Stream.of(SSLMode.values()).map(m2 -> new SSLMode[] { m1, m2 })).toArray(SSLMode[][]::new);

    @BeforeClass
    public static void initializeSelfSignedCertificateTrue() {
        System.setProperty("tcp.ssl.use.self.signed.certificate", "true");
    }

    private PgsqlProtocol startProxy(int nbBackends, SSLMode sslClientMode, SSLMode sslServerMode) throws Exception {
        SSLSessionInitializer.setClientMode(sslClientMode);
        SSLSessionInitializer.setServerMode(sslServerMode);
        PgsqlProtocol pgsqlProtocol = new PgsqlProtocol();
        pgsqlProtocol.getConfiguration()
                .setServerAddresses(Collections.nCopies(nbBackends, InetAddress.getByName("10.15.0.89")));
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.CREATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.READ, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.UPDATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.DELETE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.CREATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.READ, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.UPDATE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.DELETE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().register(new ProtocolServiceNoop());
        // pgsqlProtocol.getConfiguration().setMessagePartMaxLength(20);
        pgsqlProtocol.start();
        Thread.sleep(500);
        return pgsqlProtocol;
    }

    private Connection getConnection(boolean sslMode) throws SQLException {
        Properties info = new Properties();
        info.put("user", "postgres");
        info.put("password", "postgres");
        String url = "jdbc:postgresql://localhost:5432/ehealth";
        if (sslMode) {
            url += "?" + "ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
        }
        return DriverManager.getConnection(url, info);
    }

    private void startSession(boolean sslMode) throws SQLException {
        try (Connection con = getConnection(sslMode); Statement stmt = con.createStatement();) {
            ResultSet rs = stmt.executeQuery("select 1;");
            while (rs.next()) {
                rs.getInt(1);
            }
        }
    }

    private void testSessions(int nbBackends, SSLMode[] sslModes, int nbSessions) throws Exception {
        try (PgsqlProtocol pgsqlProtocol = startProxy(nbBackends, sslModes[0], sslModes[1]);) {
            for (int i = 0; i < nbSessions; i++) {
                if (sslModes[0] == SSLMode.DISABLED) {
                    startSession(false);
                }
                if (sslModes[0] == SSLMode.ALLOWED) {
                    startSession(false);
                    startSession(true);
                }
                if (sslModes[0] == SSLMode.REQUIRED) {
                    startSession(true);
                }
            }
        }
    }

    @Theory
    public void test1Session(@FromDataPoints("nb_backends") int nbBackends,
            @FromDataPoints("ssl_modes") SSLMode[] sslModes) throws Exception {
        testSessions(nbBackends, sslModes, 1);
    }

    @Theory
    public void test1000Sessions(@FromDataPoints("nb_backends") int nbBackends,
            @FromDataPoints("ssl_modes") SSLMode[] sslModes) throws Exception {
        testSessions(nbBackends, sslModes, 1000);
    }
}
