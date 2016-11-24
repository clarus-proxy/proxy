package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;


public class SessionHealth {

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

    @Test
    public void testSimpleSelectRequest() throws Exception{
        try(Connection con = getHealthConnection(); Statement stmt = con.createStatement();){
            String request = "SELECT * FROM PATIENT";
            ResultSet result = stmt.executeQuery(request);
            Assert.assertTrue("Result's request is empty", result.next());
        }
    }
    
    @Test
    public void testSimpleInsertRequest() throws Exception{
        try(Connection con = getHealthConnection(); Statement stmt = con.createStatement();){
            // request to check number of raw before doing insert request
            String rqstCountRawBefore = "SELECT * FROM patient";
            ResultSet resCheckBefore = stmt.executeQuery(rqstCountRawBefore);
            int rawBefore = resCheckBefore.getRow();
            // request to insert new input in table PATIENT
            String request = "INSERT INTO PATIENT VALUES ('99999999', 'TEST NAME', 'TEST LASTNAME', 'TEST LASTNAMEBIS', 'M', 'OTHER')";
            stmt.executeQuery(request);
            // request to verify if new input is inserted successfully
            String rqstCountRawAfter = "SELECT * FROM patient";
            String rqstFindInsertedRaw = "SELECT * FROM patient WHERE pat_name='TEST NAME'";
            ResultSet resCheckAfter = stmt.executeQuery(rqstCountRawAfter);
            ResultSet resCheckAfter2 = stmt.executeQuery(rqstFindInsertedRaw);
            Assert.assertEquals("Insert result's request unsuccesfull", rawBefore + 1, resCheckAfter.getRow());
            Assert.assertEquals("Row just inserted not found", 1, resCheckAfter2.getRow());
        }
    }
    
    @Test
    public void testSimpleUpdateRequest() throws Exception{
        try(Connection con = getHealthConnection(); Statement stmt = con.createStatement();){
            // select a random raw in table PATIENT
            String rqstRawRand = "SELECT * FROM patient ORDER BY RANDOM() LIMIT 1";
            ResultSet resRawRand = stmt.executeQuery(rqstRawRand);
            // get random raw selected id
            int index = resRawRand.findColumn("pat_id");
            String idRaw = null;
            if(resRawRand.next()){
                idRaw = concatenationPatientId(idRaw, resRawRand.getInt(index));
            }
            // update previous selected raw
            String rqstUpdateRaw = "UPDATE patient SET pat_name='TEST UPDATE', pat_last1='TEST LASTUPDATE', pat_last2='TEST LASTUPDATEBIS' WHERE pat_id='"+idRaw+"'";
            stmt.executeQuery(rqstUpdateRaw);
            // compare both items to check if it's different
            String rqstGetUpdatedRaw = "SELECT * FROM patient WHERE pat_id='"+idRaw+"'";
            ResultSet resGetUpdatedRaw = stmt.executeQuery(rqstGetUpdatedRaw);
            Assert.assertNotEquals("Both raw are identical, updating failed", resRawRand, resGetUpdatedRaw);
        }
    }   
    
    @Test
    public void testSimpleDeleteRequest() throws Exception{
        try(Connection con = getHealthConnection(); Statement stmt = con.createStatement();){    
            // select a random raw in table PATIENT
            String rqstRawRand = "SELECT * FROM patient ORDER BY RANDOM() LIMIT 1";
            ResultSet resRawRand = stmt.executeQuery(rqstRawRand);
            // get random raw selected id
            int index = resRawRand.findColumn("pat_id");
            String idRaw = null;
            if(resRawRand.next()){
                idRaw = concatenationPatientId(idRaw, resRawRand.getInt(index));
            }
            // delete previous selected raw
            String rqstUpdateRaw = "DELETE FROM patient WHERE pat_id='"+idRaw+"'";
            stmt.executeQuery(rqstUpdateRaw);
            // try to find previous raw in order to verify is deleting processed
            String rqstGetUpdatedRaw = "SELECT * FROM patient WHERE pat_id='"+idRaw+"'";
            ResultSet resGetUpdatedRaw = stmt.executeQuery(rqstGetUpdatedRaw);
            Assert.assertNull("Delete action unsuccesfull", resGetUpdatedRaw);
        }
    }
    
    private Connection getHealthConnection() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", "postgres");
        connectionProps.put("password", "postgres");
        return DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/ehealth",
                connectionProps);
    }
    
    private String concatenationPatientId(String idRaw, int number){
            idRaw = String.valueOf(number);
            int rawLength = idRaw.length();
            if(rawLength < 8){
                String concat = "0";
                while(concat.length() < (8 - rawLength)){
                    concat = concat.concat("0");
                }
                idRaw = concat.concat(idRaw);
            }
            return idRaw;
        }
    
}

