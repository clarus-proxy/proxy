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

public class SessionIT {

    PgsqlProtocol pgsqlProtocol = null;

    @Before
    public void startProxy() throws Exception {
        System.setProperty("pgsql.sql.force.processing", "true");
        pgsqlProtocol = new PgsqlProtocol();
        pgsqlProtocol.getConfiguration().setServerAddress(InetAddress.getByName("10.15.0.89"));
        //pgsqlProtocol.getConfiguration().setMessagePartMaxLength(20);
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
