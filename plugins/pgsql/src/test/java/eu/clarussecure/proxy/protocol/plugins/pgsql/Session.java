package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

public class Session {

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
        try (Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();) {
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

}
