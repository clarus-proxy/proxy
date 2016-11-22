package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

public class SessionIT {

    PgsqlProtocol pgsqlProtocol = null;

    @Before
    public void startProxy() throws Exception {
        pgsqlProtocol = new PgsqlProtocol();
        pgsqlProtocol.getConfiguration().setServerAddress(InetAddress.getByName("10.15.0.89"));
        //pgsqlProtocol.getConfiguration().setMessagePartMaxLength(20);
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

    @Test
    public void test1Session() throws Exception {
        try (Connection con = getConnection(); Statement stmt = con.createStatement();) {
            ResultSet rs = stmt.executeQuery("select 1;");
            while (rs.next()) {
                rs.getInt(1);
            }
        }
    }

    @Test
    public void test1000Sessions() throws Exception {
        for (int i = 0; i < 1000; i++) {
            test1Session();
        }
    }

    private Connection getConnection() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", "postgres");
        connectionProps.put("password", "postgres");

        return DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/postgres",
                connectionProps);
    }
}
