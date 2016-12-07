package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

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
    
    /**********
     * SELECT - SIMPLE QUERIES - TEST
     **********/
    @Test
    public void testSimpleSelectRequestOneParam() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT * FROM patient WHERE pat_id = ?";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "00000001");
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            Assert.assertEquals("Patient's id is not the good one" , "00000001", TestUtils.getFirstOrLastFieldValueString(res, 1, "first"));
            Assert.assertEquals("Patient's name is not the good one" , "MARIA DOLORES", TestUtils.getFirstOrLastFieldValueString(res, 2, "first"));
            Assert.assertEquals("Patient's genre is not the good one" , "F", TestUtils.getFirstOrLastFieldValueString(res, 5, "first"));
        }
    }
    
    @Test
    public void testSimpleSelectRequestSeveralParam() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT * FROM PATIENT WHERE pat_name = ? AND pat_last1 = ? AND pat_last2 = ?";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "AINA");
            prep.setString(2, "RUIZ");
            prep.setString(3, "VELASCO");
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            Assert.assertEquals("Patient's id is not the good one" , "00012345", TestUtils.getFirstOrLastFieldValueString(res, 1, "first"));
            Assert.assertEquals("Patient's last name is not the good one" , "RUIZ", TestUtils.getFirstOrLastFieldValueString(res, 3, "first"));
            Assert.assertEquals("Patient's second last name is not the good one" , "VELASCO", TestUtils.getFirstOrLastFieldValueString(res, 4, "first"));
        }
    }

    /**********
     * SELECT - ADVANCED QUERIES - ONE PARAM
     **********/
    
    @Test
    public void testSimpleSelectOneParamInt() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_id = ?";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "00000000000000000000");
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000000", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "GIN", TestUtils.getFirstOrLastFieldValueString(res, 12, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 3, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2015-04-04", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "first")));
        }
    }
    
    @Test
    public void testSimpleSelectOneParamString() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT * FROM patient WHERE pat_id = ?";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "00000001");
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            Assert.assertEquals("patient's id is not the good one" , "00000001", TestUtils.getFirstOrLastFieldValueString(res, 1, "first"));
            Assert.assertEquals("patient's name is not the good one" , "MARIA DOLORES", TestUtils.getFirstOrLastFieldValueString(res, 2, "first"));
            Assert.assertEquals("patient's lastname is not the good one" , "IGLESIAS", TestUtils.getFirstOrLastFieldValueString(res, 3, "first"));
            Assert.assertEquals("patient's genre is not the good one" , "F", TestUtils.getFirstOrLastFieldValueString(res, 5, "first"));
        }
    }
    
    @Test
    public void testSimpleSelectOneParamDate() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_adm = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            java.sql.Date date = TestUtils.instantiateNewDate("2015-04-04");
            prep.setDate(1, date);
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000000", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "GIN", TestUtils.getFirstOrLastFieldValueString(res, 12, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 10, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-04-14", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    /*
     * SELECT - ADVANCED QUERIES - TWO PARAM
     */
    
    @Test
    public void testSimpleSelectTwoParamStringAndInt() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_serv = ? AND dis_days = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "OFT");
            prep.setInt(2, 21);
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000022", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "00", TestUtils.getFirstOrLastFieldValueString(res, 11, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 21, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-10-15", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "first")));
            // test on last row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000034011", TestUtils.getFirstOrLastFieldValueString(res, 10, "last"));
            Assert.assertEquals("Discharge's serv is not the good one" , "OFT", TestUtils.getFirstOrLastFieldValueString(res, 12, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 21, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2015-11-25", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectTwoParamStringAndDate() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_serv = ? AND dis_adm = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "OFT");
            prep.setDate(2, TestUtils.instantiateNewDate("2015-03-02"));
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000540", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "00", TestUtils.getFirstOrLastFieldValueString(res, 11, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 15, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-03-17", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "first")));
            // test on last row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000033903", TestUtils.getFirstOrLastFieldValueString(res, 10, "last"));
            Assert.assertEquals("Discharge's serv is not the good one" , "OFT", TestUtils.getFirstOrLastFieldValueString(res, 12, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 29, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-03-31", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectTwoParamIntAndDate() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_days = ? AND dis_adm = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setInt(1, 10);
            prep.setDate(2, TestUtils.instantiateNewDate("2015-03-02"));
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000462", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "00", TestUtils.getFirstOrLastFieldValueString(res, 11, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 10, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-03-12", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "first")));
            // test on last row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000033804", TestUtils.getFirstOrLastFieldValueString(res, 10, "last"));
            Assert.assertEquals("Discharge's serv is not the good one" , "GIN", TestUtils.getFirstOrLastFieldValueString(res, 12, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 10, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-03-12", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectTwoParamOperatorBetweenTwoDate() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_adm BETWEEN ? AND ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setDate(1, TestUtils.instantiateNewDate("2015-02-01"));
            prep.setDate(2, TestUtils.instantiateNewDate("2015-02-02"));
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000135", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "00", TestUtils.getFirstOrLastFieldValueString(res, 11, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 19, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-02-20", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "first")));
            // test on last row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000033965", TestUtils.getFirstOrLastFieldValueString(res, 10, "last"));
            Assert.assertEquals("Discharge's serv is not the good one" , "GIN", TestUtils.getFirstOrLastFieldValueString(res, 12, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 28, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-03-01", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    /*
     * SELECT - ADVANCED QUERIES - THREE PARAM
     */
    
    @Test
    public void testSimpleSelectThreeParamStringAndStringAndInt() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_serv = ? AND dis_sig1 = ? AND dis_days = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "OFT");
            prep.setString(2, "000046");
            prep.setInt(3, 21);
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000022", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "00", TestUtils.getFirstOrLastFieldValueString(res, 11, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 21, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2015-09-24", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "first")));
            // test on last row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000030683", TestUtils.getFirstOrLastFieldValueString(res, 10, "last"));
            Assert.assertEquals("Discharge's serv is not the good one" , "OFT", TestUtils.getFirstOrLastFieldValueString(res, 12, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 21, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2015-02-08", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectThreeParamStringAndStringAndDate() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_serv = ? AND dis_adtp = ? AND dis_adm = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "OFT");
            prep.setString(2, "EN");
            prep.setDate(3, TestUtils.instantiateNewDate("2015-02-02"));
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000241", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's ver is not the good one" , "00", TestUtils.getFirstOrLastFieldValueString(res, 11, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 22, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-02-24", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "first")));
            // test on last row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000033720", TestUtils.getFirstOrLastFieldValueString(res, 10, "last"));
            Assert.assertEquals("Discharge's serv is not the good one" , "OFT", TestUtils.getFirstOrLastFieldValueString(res, 12, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 3, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-02-05", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectThreeParamStringAndDateAndDate() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE dis_ver = ? AND dis_adm = ? AND dis_dis = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "00");
            prep.setDate(2, TestUtils.instantiateNewDate("2015-02-02"));
            prep.setDate(3, TestUtils.instantiateNewDate("2015-02-03"));
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000004908", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "00", TestUtils.getFirstOrLastFieldValueString(res, 11, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 1, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-02-03", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "first")));
            // test on last row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000021883", TestUtils.getFirstOrLastFieldValueString(res, 10, "last"));
            Assert.assertEquals("Discharge's serv is not the good one" , "HEM", TestUtils.getFirstOrLastFieldValueString(res, 12, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 1, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-02-03", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    /*
     * SELECT - ADVANCED QUERIES - FOUR PARAM
     */
    
    @Test
    public void testSimpleSelectThreeParamSSSS() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE pat_gen = ? AND ep_range = ? AND dis_serv = ? AND dis_adtp = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "F");
            prep.setString(2, "14");
            prep.setString(3, "GIN");
            prep.setString(4, "PR");
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000000047", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "GIN", TestUtils.getFirstOrLastFieldValueString(res, 12, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 19, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2015-06-14", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "first")));
            // test on last row
            Assert.assertEquals("Patient's id is not the good one" , "00017675", TestUtils.getFirstOrLastFieldValueString(res, 1, "last"));
            Assert.assertEquals("Patient's name is not the good one" , "MARIA", TestUtils.getFirstOrLastFieldValueString(res, 2, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 7, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-06-29", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectThreeParamSSSI() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE pat_gen = ? AND ep_range = ? AND dis_serv = ? AND dis_days = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "F");
            prep.setString(2, "14");
            prep.setString(3, "GIN");
            prep.setInt(4, 10);
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000009154", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "GIN", TestUtils.getFirstOrLastFieldValueString(res, 12, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 10, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2015-10-09", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "first")));
            // test on last row
            Assert.assertEquals("Patient's id is not the good one" , "00015614", TestUtils.getFirstOrLastFieldValueString(res, 1, "last"));
            Assert.assertEquals("Patient's name is not the good one" , "DOLORES", TestUtils.getFirstOrLastFieldValueString(res, 2, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 10, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-11-18", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectThreeParamSSSD() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE pat_gen= ? AND ep_range= ? AND dis_serv= ? AND dis_adm= ?;";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "F");
            prep.setString(2, "14");
            prep.setString(3, "GIN");
            prep.setDate(4, TestUtils.instantiateNewDate("2015-02-02"));
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000020701", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("patient's id is not the good one" , "00008278", TestUtils.getFirstOrLastFieldValueString(res, 1, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 11, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-02-13", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "first")));
        }
    }
    
    @Test
    public void testSimpleSelectThreeParamSSII() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE pat_gen = ? AND ep_range = ? AND ep_age = ? AND dis_days = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "F");
            prep.setString(2, "14");
            prep.setInt(3, 62);
            prep.setInt(4, 10);
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000003404", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "HEM", TestUtils.getFirstOrLastFieldValueString(res, 12, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 10, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2016-01-10", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "first")));
            // test on last row
            Assert.assertEquals("Patient's id is not the good one" , "00018623", TestUtils.getFirstOrLastFieldValueString(res, 1, "last"));
            Assert.assertEquals("Patient's name is not the good one" , "MARIA ROSA", TestUtils.getFirstOrLastFieldValueString(res, 2, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 10, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-08-07", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }
    
    @Test
    public void testSimpleSelectThreeParamSSDD() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            String request = "SELECT DISTINCT ON (dis_id, dis_ver) * FROM discharge_advanced WHERE pat_gen = ? AND ep_range = ? AND dis_adm = ? AND dis_dis = ? ORDER BY dis_id";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            prep.setString(1, "F");
            prep.setString(2, "14");
            prep.setDate(3, TestUtils.instantiateNewDate("2015-02-09"));
            prep.setDate(4, TestUtils.instantiateNewDate("2015-02-14"));
            ResultSet res = prep.executeQuery();
            Assert.assertNotNull("Result's request is empty", res);
            // test on first row
            Assert.assertEquals("Discharge's id is not the good one" , "00000000000000020028", TestUtils.getFirstOrLastFieldValueString(res, 10, "first"));
            Assert.assertEquals("Discharge's serv is not the good one" , "GIN", TestUtils.getFirstOrLastFieldValueString(res, 12, "first"));
            Assert.assertEquals("Discharge's day is not the good one" , 5, TestUtils.getFirstOrLastFieldValueInt(res, 15, "first"));
            Assert.assertEquals("Discharge's adm is not the good one" , "2015-02-09", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 13, "first")));
            // test on last row
            Assert.assertEquals("Patient's id is not the good one" , "00006589", TestUtils.getFirstOrLastFieldValueString(res, 1, "last"));
            Assert.assertEquals("Patient's name is not the good one" , "MONTSERRAT", TestUtils.getFirstOrLastFieldValueString(res, 2, "last"));
            Assert.assertEquals("Discharge's day is not the good one" , 5, TestUtils.getFirstOrLastFieldValueInt(res, 15, "last"));
            Assert.assertEquals("Discharge's dis is not the good one" , "2015-02-14", TestUtils.convertDateToString(TestUtils.getFirstOrLastFieldValueDate(res, 14, "last")));
        }
    }

    @Test
    public void testSimpleInsertRequest() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);){
            // request to check number of row before doing insert request
            String rqstCountrowBefore = "SELECT * FROM patient ORDER BY pat_id";
            ResultSet resCheckBefore = stmt.executeQuery(rqstCountrowBefore);
            int rowBefore = TestUtils.getRowCount(resCheckBefore);
            // create new random (String) patient's id & name
            String newId = TestUtils.createNewIdByIncrement(resCheckBefore);
            String newName = TestUtils.generateRandomString();
            // request to insert new input in table PATIENT
            String request = "INSERT INTO PATIENT VALUES ('"+newId+"', '"+newName+"', 'TEST LASTNAME', 'TEST LASTNAMEBIS', 'M', 'OTHER')";
            stmt.execute(request);
            // request to verify if new input is inserted successfully
            String rqstCountrowAfter = "SELECT * FROM patient";
            ResultSet resCheckAfter = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(rqstCountrowAfter);
            int rowAfter = TestUtils.getRowCount(resCheckAfter);
            Assert.assertEquals("Insert result's request unsuccesfull", rowBefore + 1, rowAfter);
            String rqstFindInsertedRow = "SELECT * FROM patient WHERE pat_name='"+newName+"'";
            ResultSet resCheckAfter2 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT).executeQuery(rqstFindInsertedRow);
            Assert.assertEquals("Row just inserted not found", newId, TestUtils.getFirstOrLastFieldValueString(resCheckAfter2, 1, "last"));
        }
    }
    
    @Test
    public void testSimpleUpdateRequest() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){
            // select a random row in table PATIENT
            String rqstRowRand = "SELECT * FROM patient ORDER BY RANDOM() LIMIT 1";
            ResultSet resRowRand = stmt.executeQuery(rqstRowRand);
            // get random row selected id
            int index = resRowRand.findColumn("pat_id");
            String idrow = null;
            if(resRowRand.next()){
                idrow = TestUtils.concatenationZeroIntAsString(resRowRand.getInt(index), 8);
            }
            // update previous selected row
            String rqstUpdaterow = "UPDATE patient SET pat_name='TEST UPDATE', pat_last1='TEST LASTUPDATE', pat_last2='TEST LASTUPDATEBIS' WHERE pat_id='"+idrow+"'";
            stmt.execute(rqstUpdaterow);
            // compare both items to check if it's different
            String rqstGetUpdatedrow = "SELECT * FROM patient WHERE pat_id='"+idrow+"'";
            ResultSet resGetUpdatedrow = stmt.executeQuery(rqstGetUpdatedrow);
            Assert.assertNotEquals("Both row are identical, updating failed", resRowRand, resGetUpdatedrow);
        }
    }   
    
    @Test
    public void testSimpleDeleteRequest() throws Exception{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement();){    
            // select a random row in table PATIENT
            String rqstRowRand = "SELECT * FROM patient ORDER BY RANDOM() LIMIT 1";
            ResultSet resRowRand = stmt.executeQuery(rqstRowRand);
            // get random row selected id
            int index = resRowRand.findColumn("pat_id");
            String idrow = null;
            if(resRowRand.next()){
                idrow = TestUtils.concatenationZeroIntAsString(resRowRand.getInt(index), 8);
            }
            // delete previous selected row
            String rqstUpdaterow = "DELETE FROM patient WHERE pat_id='"+idrow+"'";
            stmt.execute(rqstUpdaterow);
            // try to find previous row in order to verify is deleting processed
            String rqstGetUpdatedrow = "SELECT * FROM patient WHERE pat_id='"+idrow+"'";
            ResultSet resGetUpdatedrow = stmt.executeQuery(rqstGetUpdatedrow);
            Assert.assertEquals("Delete action unsuccesfull", 0, resGetUpdatedrow.getRow());
        }
    }
    
}

