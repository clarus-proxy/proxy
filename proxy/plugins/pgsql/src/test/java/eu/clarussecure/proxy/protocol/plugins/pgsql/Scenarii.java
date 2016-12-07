package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.dbcp.ConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

public class Scenarii {

    PgsqlProtocol pgsqlProtocol = null;
    private final int insertRequestNumber5 = 5;
    private final int insertRequestNumber10 = 10;
    private final int insertRequestNumber25 = 25;
    private final int insertRequestNumber50 = 50;
    private final int insertRequestNumber100 = 100;

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
    
    /*
     * TEST EXECUTE - LOOP ON X EXECUTE INSERT REQUEST
     */
    
    @Test
    public void executeInsertRow5() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber5);
    }
    
    @Test
    public void executeInsertRow10() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber10);
    }
    
    @Test
    public void executeInsertRow25() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber25);
    }
    
    @Test
    public void executeInsertRow50() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber50);
    }
    
    @Test
    public void executeInsertRow100() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber100);
    }
    
    /*
     * TEST EXECUTE - EXECUTE ONE INSERT REQUEST WITH X VALUES SET
     */
    
    @Test
    public void executeInsertAllRow5() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber5);
    }
    
    @Test
    public void executeInsertAllRow10() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber10);
    }
    
    @Test
    public void executeInsertAllRow25() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber25);
    }
    
    @Test
    public void executeInsertAllRow50() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber50);
    }
    
    @Test
    public void executeInsertAllRow100() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber100);
    }
    
    /*
     * TEST PARSE - GENERATE X PREPARED STATEMENT (INSERT REQUEST)
     */
    
    @Test
    public void parseInsertRow5() throws SQLException{
        parseInsertRequestRow(insertRequestNumber5);
    }
    
    @Test
    public void parseInsertRow10() throws SQLException{
        parseInsertRequestRow(insertRequestNumber10);
    }
    
    @Test
    public void parseInsertRow25() throws SQLException{
        parseInsertRequestRow(insertRequestNumber25);
    }
    
    @Test
    public void parseInsertRow50() throws SQLException{
        parseInsertRequestRow(insertRequestNumber50);
    }
    
    @Test
    public void parseInsertRow100() throws SQLException{
        parseInsertRequestRow(insertRequestNumber100);
    }
    
    /*
     * TEST BIND - BIND VALUES OF X PREPARED STATEMENT (INSERT REQUEST)
     */
    
    @Test
    public void bindInsertRow5() throws SQLException{
        bindInsertRequestRow(insertRequestNumber5);
    }
    
    @Test
    public void bindInsertRow10() throws SQLException{
        bindInsertRequestRow(insertRequestNumber10);
    }
    
    @Test
    public void bindInsertRow25() throws SQLException{
        bindInsertRequestRow(insertRequestNumber25);
    }
    
    @Test
    public void bindInsertRow50() throws SQLException{
        bindInsertRequestRow(insertRequestNumber50);
    }
    
    @Test
    public void bindInsertRow100() throws SQLException{
        bindInsertRequestRow(insertRequestNumber100);
    }
    
    /*
     * TEST SCENARIO - RANDOM PARSE/BIND/EXECUTE
     */
    
    @Test
    public void parseBindExecuteScenario5() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber5);
    }
    
    @Test
    public void parseBindExecuteScenario10() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber10);
    }
    
    @Test
    public void parseBindExecuteScenario25() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber25);
    }
    
    @Test
    public void parseBindExecuteScenario50() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber50);
    }

    @Test
    public void parseBindExecuteScenario100() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber100);
    }
    
    /*
     * SCENARII METHOD'S TEST
     */
    
    private void insertParametredRequestLoop(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = TestUtils.createStatement(con);){
            // check number of row before insert
            String rqstCountrowBefore = "SELECT * FROM patient ORDER BY pat_id";
            ResultSet resCheckBefore = stmt.executeQuery(rqstCountrowBefore);
            int rowBefore = TestUtils.getRowCount(resCheckBefore);
            Assert.assertNotNull("Request's result is null", resCheckBefore);
            // insert X row
            List<String> lstRequest = TestUtils.generateInsertRequest(numberOfRequest);
            Iterator<String> iterator = lstRequest.iterator();
            while(iterator.hasNext()){
                stmt.execute(iterator.next());
            }
            // check number of row after insert
            String rqstCountrowAfter = "SELECT * FROM patient ORDER BY pat_id";
            ResultSet resCheckAfter = stmt.executeQuery(rqstCountrowAfter);
            int rowAfter = TestUtils.getRowCount(resCheckAfter);
            Assert.assertNotNull("Request's result is null", resCheckBefore);
            Assert.assertEquals("Insert result's request unsuccesfull", rowBefore + numberOfRequest, rowAfter);
        }
    }
    
    private void insertParametredRequestOneShot(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = TestUtils.createStatement(con);){    
            // check number of row before insert
            String rqstCountrowBefore = "SELECT * FROM patient ORDER BY pat_id";
            ResultSet resCheckBefore = stmt.executeQuery(rqstCountrowBefore);
            int rowBefore = TestUtils.getRowCount(resCheckBefore);
            Assert.assertNotNull("Request's result is null", resCheckBefore);
            // insert X row
            String requestAll = TestUtils.generateInsertAllRequest(numberOfRequest);
            stmt.execute(requestAll);
            // check number of row after insert
            String rqstCountrowAfter = "SELECT * FROM patient ORDER BY pat_id";
            ResultSet resCheckAfter = stmt.executeQuery(rqstCountrowAfter);
            int rowAfter = TestUtils.getRowCount(resCheckAfter);
            Assert.assertNotNull("Request's result is null", resCheckBefore);
            Assert.assertEquals("Insert result's request unsuccesfull", rowBefore + numberOfRequest, rowAfter);
        }
    }
    
    private void parseInsertRequestRow(int numberOfRequest) throws SQLException{
        List<PreparedStatement> lstPrepStmt = TestUtils.generateInsertPreparedStatementRequest(numberOfRequest);
        if(numberOfRequest != 0){
            Assert.assertNotNull("List of PreparedStatement should not be null", lstPrepStmt);
            Assert.assertEquals("Number of Statement should be equals to number of request parameter", numberOfRequest, lstPrepStmt.size());
        }
    }
    
    private void parseRequestSuccessfullTest(int numberOfRequest, List<PreparedStatement> lstPrepStmt) throws SQLException{
        if(numberOfRequest != 0){
            Assert.assertNotNull("List of PreparedStatement should not be null", lstPrepStmt);
            Assert.assertEquals("Number of Statement should be equals to number of request parameter", numberOfRequest, lstPrepStmt.size());
        }
    }
    
    private void bindInsertRequestRow(int numberOfRequest) throws SQLException{
        List<PreparedStatement> lstPreparedStmtBinded = TestUtils.generateInsertBindRequest(numberOfRequest);
        if(numberOfRequest != 0){
            Assert.assertNotNull("List of PreparedStatement should not be null", lstPreparedStmtBinded);
            Assert.assertEquals("Number of Statement should be equals to number of request parameter", numberOfRequest, lstPreparedStmtBinded.size());
            // check if all bind work succesfully and no "?" still in statement
            Iterator<PreparedStatement> iterator = lstPreparedStmtBinded.iterator();
            while(iterator.hasNext()){
                String request = String.valueOf(iterator.next());
                int stringSize = request.length();
                int counter = 0;
                while(counter < stringSize){
                    if(request.indexOf('?') != -1){
                        Assert.assertNotEquals("Special character ? find, it means that binding didn't work correctly", -1, request.indexOf('?'));
                        break;
                    }
                    else{
                        counter = counter + 1;
                    }
                }
            }
        }
    }
    
    private void bindRequestSuccessfullTest(int numberOfRequest, List<PreparedStatement> lstPrepStmt) throws SQLException{
        if(numberOfRequest != 0){
            Assert.assertNotNull("List of PreparedStatement should not be null", lstPrepStmt);
            Assert.assertEquals("Number of Statement should be equals to number of request parameter", numberOfRequest, lstPrepStmt.size());
            // check if all bind work succesfully and no "?" still in statement
            Iterator<PreparedStatement> iterator = lstPrepStmt.iterator();
            while(iterator.hasNext()){
                String request = String.valueOf(iterator.next());
                int stringSize = request.length();
                int counter = 0;
                while(counter < stringSize){
                    if(request.indexOf('?') != -1){
                        Assert.assertNotEquals("Special character ? find, it means that binding didn't work correctly", -1, request.indexOf('?'));
                        break;
                    }
                    else{
                        counter = counter + 1;
                    }
                }
            }
        }
    }
    
    private void parseBindExecuteRandomScenario(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection();){
            // check number of row before insert
            Statement stmt = TestUtils.createStatement(con);
            int rowBefore = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Table patient shall be not empty", rowBefore);
            // insert X row in table PATIENT with random values
            List<String> lstInsertRequest = TestUtils.generateInsertRequest(numberOfRequest);
            Assert.assertNotNull("This list of insert request shall be not empty", lstInsertRequest);
            List<String> lstIdInserted = new ArrayList<String>();
            Iterator<String> iterator = lstInsertRequest.iterator();
            int counter1 = 1;
            while(iterator.hasNext()){
                String request = iterator.next();
                stmt.execute(request);
                Assert.assertEquals("Table patient shall contains "+(rowBefore + counter1)+" rows", (rowBefore + counter1), TestUtils.getNumberOfRow(stmt, "patient", "pat_id"));
                counter1 ++;
                lstIdInserted.add((String) request.subSequence(29, 37));
            }
            Assert.assertEquals("Table patient shall contains "+(rowBefore + numberOfRequest)+" rows", (rowBefore + numberOfRequest), TestUtils.getNumberOfRow(stmt, "patient", "pat_id"));
            // check number of row after insert
            int rowAfter = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Table patient shall be not empty", rowAfter);
            Assert.assertEquals("Insert result's request unsuccesfull", rowBefore + numberOfRequest, rowAfter);
            // parse (X x 2) insert request
            List<PreparedStatement> lstPrepStmtInsert = TestUtils.generateInsertPreparedStatementRequest(numberOfRequest*2);
            parseRequestSuccessfullTest(numberOfRequest*2, lstPrepStmtInsert);
            // parse X update request
            List<PreparedStatement> lstPrepStmtUpdate = TestUtils.generateUpdatePreparedStatementRequest(numberOfRequest);
            parseRequestSuccessfullTest(numberOfRequest, lstPrepStmtUpdate);
            // bind Y (random) update request
            int nbrOfBind = TestUtils.generateRandomInt(1, numberOfRequest);
            List<PreparedStatement> lstBindedUpdate = TestUtils.bindUpdateRequest(nbrOfBind, lstPrepStmtUpdate, lstIdInserted);
            bindRequestSuccessfullTest(nbrOfBind, lstBindedUpdate);
            // execute Y update request
            Iterator<PreparedStatement> iterator2 = lstBindedUpdate.iterator();
            while(iterator2.hasNext()){
                PreparedStatement prepStmt = iterator2.next();
                String rowBeforUpdate = getRowBeforeUpdate(con, prepStmt);
                stmt.execute(String.valueOf(iterator2.next()));
                updateRequestTestSuccesfull(con, prepStmt, rowBeforUpdate);
            }
            // bind X insert
            List<PreparedStatement> lstPrepStmtInsertToBind = new ArrayList<PreparedStatement>();
            Iterator<PreparedStatement> iterator3 = lstPrepStmtInsert.iterator();
            int counter2 = 0;
            while(iterator3.hasNext() && counter2 < numberOfRequest){
                lstPrepStmtInsertToBind.add(iterator3.next());
                iterator3.remove();
                counter2 = counter2 + 1;
            }
            List<PreparedStatement> lstBindedInsert = TestUtils.bindInsertRequest(lstPrepStmtInsertToBind.size(), lstPrepStmtInsertToBind);
            bindRequestSuccessfullTest(numberOfRequest, lstBindedInsert);
            // execute X binded insert
            Iterator<PreparedStatement> iterator4 = lstBindedInsert.iterator();
            int counter3 = 1;
            while(iterator4.hasNext()){
                stmt.execute(String.valueOf(iterator4.next()));
                Assert.assertEquals("Table patient shall contains "+(rowAfter + counter3)+" rows", (rowAfter + counter3), TestUtils.getNumberOfRow(stmt, "patient", "pat_id"));
                counter3 ++;
            }
            // bind other insert (never executed)
            List<PreparedStatement> lstOtherBindedInsert = TestUtils.bindInsertRequest(lstPrepStmtInsert.size(), lstPrepStmtInsert);
            bindRequestSuccessfullTest(numberOfRequest, lstOtherBindedInsert);
            // parse + bind delete (all row inserted)
            String reqCountRow = "SELECT * FROM patient ORDER BY pat_id";
            ResultSet resCountRow = stmt.executeQuery(reqCountRow);
            Assert.assertNotNull("Table patient should not be empty", resCountRow);
            resCountRow.last();
            String lastNewValue = TestUtils.getFirstOrLastFieldValueString(resCountRow, 1, "last");
            String firstNewValue = TestUtils.concatenationZeroIntAsString(Integer.parseInt(lastNewValue) - (numberOfRequest * 2) + 1, 8);
            // execute all delete
            String reqDelete = "DELETE FROM PATIENT WHERE pat_id BETWEEN '"+ firstNewValue +"' AND '"+lastNewValue+"'";
            stmt.execute(reqDelete);
            String rqstCountRowEnd = "SELECT * FROM patient ORDER BY pat_id";
            ResultSet resCheckEnd = stmt.executeQuery(rqstCountRowEnd);
            int rowEnd = TestUtils.getRowCount(resCheckEnd);
            Assert.assertEquals("Table patient shall contains "+(rowAfter + numberOfRequest - (numberOfRequest*2))+" rows", (rowAfter + numberOfRequest - (numberOfRequest*2)), rowEnd);
            stmt.close();
        }
    }
    
    private void updateRequestTestSuccesfull(Connection con, PreparedStatement prepStmt, String requestBefore) throws SQLException{
        String request = String.valueOf(prepStmt);
        int index = request.indexOf("pat_id") + 10;
        String id = request.substring(index, index + 8); 
        Statement statement = TestUtils.createStatement(con);
        String req = "SELECT * FROM patient WHERE pat_id = '"+id+"'";
        ResultSet res = statement.executeQuery(req);
        Assert.assertNotEquals("Row update unsuccesfull", String.valueOf(res), requestBefore);
    }
    
    private String getRowBeforeUpdate(Connection con, PreparedStatement prepStmt) throws SQLException{
        String request = String.valueOf(prepStmt);
        int index = request.indexOf("pat_id") + 10;
        String id = request.substring(index, index + 8); 
        Statement statement = TestUtils.createStatement(con);
        String req = "SELECT * FROM patient WHERE pat_id = '"+id+"'";
        ResultSet res = statement.executeQuery(req);
        return String.valueOf(res);
    }
    
}
    
    
//  @Test
//  public void insertPreparedStmtPatientRow5() throws SQLException{
//      insertPreparedStatementRequest(insertRequestNumber5);
//  }
//  
//  @Test
//  public void insertPreparedStmtPatientRow10() throws SQLException{
//      insertPreparedStatementRequest(insertRequestNumber10);
//  }
//  
//  @Test
//  public void insertPreparedStmtPatientRow25() throws SQLException{
//      insertPreparedStatementRequest(insertRequestNumber25);
//  }
//  
//  @Test
//  public void insertPreparedStmtPatientRow50() throws SQLException{
//      insertPreparedStatementRequest(insertRequestNumber50);
//  }
//  
//  @Test
//  public void insertPreparedStmtPatientRow100() throws SQLException{
//      insertPreparedStatementRequest(insertRequestNumber100);
//  }
    
    
//    private void insertPreparedStatementRequest(int numberOfRequest) throws SQLException{
//        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
//                ResultSet.CONCUR_READ_ONLY, 
//                ResultSet.HOLD_CURSORS_OVER_COMMIT);){    
//            // check number of row before insert
//            String rqstCountRowBefore = "SELECT * FROM patient ORDER BY pat_id";
//            ResultSet resCheckBefore = stmt.executeQuery(rqstCountRowBefore);
//            int rowBefore = TestUtils.getRowCount(resCheckBefore);
//            Assert.assertNotNull("Request's result is null", resCheckBefore);
//            // insert X row
////            Map<String, List<PreparedStatement>> allRequest = TestUtils.test(numberOfRequest);
//            /*
//             * 
//             * 
//             * 
//             */
//            Map<String, List<PreparedStatement>> myMap = new HashMap<String, List<PreparedStatement>>();
//            List<PreparedStatement> lstPreparedStmt = new ArrayList<PreparedStatement>();
//            String request = "INSERT INTO PATIENT VALUES (?, ?, ?, ?, 'M', 'OTHER')";
//            myMap.put(request, null);
//            int counter = 0;
//            String rqstSelect = "SELECT * FROM patient ORDER BY pat_id"; //initialize SELECT request
//            ResultSet resRequest = stmt.executeQuery(rqstSelect); //execute SELECT
//            String newId = TestUtils.createNewIdByIncrement(resRequest); //initialize first new id
//            while(counter < numberOfRequest){
//                String newName = TestUtils.generateRandomString(); //generate random new name
//                String newLast = TestUtils.generateRandomString(); //generate random new last name
//                String newLast2 = TestUtils.generateRandomString(); //generate random new last name 2
//                PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
//                        ResultSet.CONCUR_READ_ONLY,
//                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
//                prep.setString(1, newId);
//                prep.setString(2, newName);
//                prep.setString(3, newLast);
//                prep.setString(4, newLast2);
//                lstPreparedStmt.add(prep);
//                counter = counter + 1; //increment counter
//                int newIdAsInt = Integer.parseInt(newId) + 1; //increment id
//                newId = TestUtils.concatenationZeroIntAsString(newIdAsInt, 8);
//            }
//            myMap.put(request, lstPreparedStmt);
//            /*
//             * 
//             * 
//             * 
//             */
//            for(Map.Entry<String, List<PreparedStatement>> e : myMap.entrySet()){
////                List<PreparedStatement> lstStatement = e.getValue();
//                Iterator<PreparedStatement> valIterator = e.getValue().iterator();
//                while(valIterator.hasNext()){
//                    valIterator.next().execute();
//                }
//            }
//            // check number of row after insert
//            String rqstCountrowAfter = "SELECT * FROM patient ORDER BY pat_id";
//            ResultSet resCheckAfter = stmt.executeQuery(rqstCountrowAfter);
//            int rowAfter = TestUtils.getRowCount(resCheckAfter);
//            Assert.assertNotNull("Request's result is null", resCheckBefore);
//            Assert.assertEquals("Insert result's request unsuccesfull", rowBefore + numberOfRequest, rowAfter);
//        }
//    }

