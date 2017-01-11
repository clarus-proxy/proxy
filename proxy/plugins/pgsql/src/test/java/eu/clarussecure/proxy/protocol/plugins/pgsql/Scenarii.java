package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

/**
 * @author Mehdi.BENANIBA
 *
 */
public class Scenarii {

    PgsqlProtocol pgsqlProtocol = null;
    private final int insertRequestNumber5 = 5;
    private final int insertRequestNumber10 = 10;
    private final int insertRequestNumber25 = 25;
    private final int insertRequestNumber50 = 50;
    private final int insertRequestNumber100 = 100;

    @Before
    public void startProxy() throws Exception {
        System.setProperty("pgsql.sql.force.processing", "true");
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
        deleteRowInserted(insertRequestNumber5);
    }

    @Test
    public void executeInsertRow10() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber10);
        deleteRowInserted(insertRequestNumber10);
    }

    @Test
    public void executeInsertRow25() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber25);
        deleteRowInserted(insertRequestNumber25);
    }

    @Test
    public void executeInsertRow50() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber50);
        deleteRowInserted(insertRequestNumber50);
    }

    @Test
    public void executeInsertRow100() throws SQLException{
        insertParametredRequestLoop(insertRequestNumber100);
        deleteRowInserted(insertRequestNumber100);
    }

    /*
     * TEST EXECUTE - EXECUTE ONE INSERT REQUEST WITH X VALUES SET
     */

    @Test
    public void executeInsertAllRow5() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber5);
        deleteRowInserted(insertRequestNumber5);
    }

    @Test
    public void executeInsertAllRow10() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber10);
        deleteRowInserted(insertRequestNumber10);
    }

    @Test
    public void executeInsertAllRow25() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber25);
        deleteRowInserted(insertRequestNumber25);
    }

    @Test
    public void executeInsertAllRow50() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber50);
        deleteRowInserted(insertRequestNumber50);
    }

    @Test
    public void executeInsertAllRow100() throws SQLException{
        insertParametredRequestOneShot(insertRequestNumber100);
        deleteRowInserted(insertRequestNumber100);
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
     * TEST SCENARION - LOGIC PARSE/BINDE/EXECUTE
     */

    @Test
    public void parseBindExecuteLogicScenario5() throws SQLException{
        parseBindExecuteLogicScenario(insertRequestNumber5);
    }

    @Test
    public void parseBindExecuteLogicScenario10() throws SQLException{
        parseBindExecuteLogicScenario(insertRequestNumber10);
    }

    @Test
    public void parseBindExecuteLogicScenario25() throws SQLException{
        parseBindExecuteLogicScenario(insertRequestNumber25);
    }

//    @Test
//    public void parseBindExecuteLogicScenario50() throws SQLException{
//        parseBindExecuteLogicScenario(insertRequestNumber50);
//    }
//
//    @Test
//    public void parseBindExecuteLogicScenario100() throws SQLException{
//        parseBindExecuteLogicScenario(insertRequestNumber100);
//    }

    /*
     * TEST SCENARIO - RANDOM PARSE/BIND/EXECUTE
     */

    @Test
    public void parseBindExecuteRandomScenario5() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber5);
    }

    @Test
    public void parseBindExecuteRandomScenario10() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber10);
    }

    @Test
    public void parseBindExecuteRandomScenario25() throws SQLException{
        parseBindExecuteRandomScenario(insertRequestNumber25);
    }

//    @Test
//    public void parseBindExecuteRandomScenario50() throws SQLException{
//        parseBindExecuteRandomScenario(insertRequestNumber50);
//    }
//
//    @Test
//    public void parseBindExecuteRandomScenario100() throws SQLException{
//        parseBindExecuteRandomScenario(insertRequestNumber100);
//    }

    /*
     * SCENARII METHOD'S TEST
     */

    /**
     * method which insert row in patient value. this insert process over and over until number of request insert equals (int) method's parameter.
     * Moreover, this insert's success is check by some test.
     * @param numberOfRequest
     * @throws SQLException
     */
    private void insertParametredRequestLoop(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = TestUtils.createStatement(con);){
            // check number of row before insert
            int rowBefore = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Request's result is empty", rowBefore);
            // insert X row
            List<String> lstRequest = TestUtils.generateInsertRequest(numberOfRequest);
            Iterator<String> iterator = lstRequest.iterator();
            while(iterator.hasNext()){
                stmt.execute(iterator.next());
            }
            // check number of row after insert
            int rowAfter = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Request's result is empty", rowAfter);
            Assert.assertEquals("Insert result's request unsuccesfull", rowBefore + numberOfRequest, rowAfter);
            TestUtils.closeAll(stmt, con);
        }
    }

    /**
     * method which insert row in patient value. Number of row is equals (int) method's parameter.
     * Moreover, this insert's success is check by some test.
     * @param numberOfRequest
     * @throws SQLException
     */
    private void insertParametredRequestOneShot(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = TestUtils.createStatement(con);){
            // check number of row before insert
            int rowBefore = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Request's result is empty", rowBefore);
            // insert X row
            String requestAll = TestUtils.generateInsertAllRequest(numberOfRequest);
            stmt.execute(requestAll);
            // check number of row after insert
            int rowAfter = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Request's result is empty", rowAfter);
            Assert.assertEquals("Insert result's request unsuccesfull", rowBefore + numberOfRequest, rowAfter);
            TestUtils.closeAll(stmt, con);
        }
    }

    /**
     * method which parse INSERT request.
     * Moreover, this parse success is check by some test.
     * @param numberOfRequest
     * @throws SQLException
     */
    private void parseInsertRequestRow(int numberOfRequest) throws SQLException{
        List<PreparedStatement> lstPrepStmt = TestUtils.generateInsertPreparedStatementRequest(numberOfRequest);
        if(numberOfRequest != 0){
            Assert.assertNotNull("List of PreparedStatement should not be null", lstPrepStmt);
            Assert.assertEquals("Number of Statement should be equals to number of request parameter", numberOfRequest, lstPrepStmt.size());
        }
    }

    /**
     * method which checks if parse methods processed successfully
     * Moreover, this parse success is check by some test .
     * @param numberOfRequest
     * @param lstPrepStmt
     * @throws SQLException
     */
    private void parseRequestSuccessfullTest(int numberOfRequest, List<PreparedStatement> lstPrepStmt) throws SQLException{
        if(numberOfRequest != 0){
            Assert.assertNotNull("List of PreparedStatement should not be null", lstPrepStmt);
            Assert.assertEquals("Number of Statement should be equals to number of request parameter", numberOfRequest, lstPrepStmt.size());
        }
    }

    /**
     * method which bind INSERT request.
     * Moreover, this binding success is check by some test
     * @param numberOfRequest
     * @throws SQLException
     */
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

    /**
     * method which checks if bind methods processed successfully
     * @param numberOfRequest
     * @param lstPrepStmt
     * @throws SQLException
     */
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

    /**
     * method which delete row and check if it processed well
     * @param numberOfRequest
     * @throws SQLException
     */
    private void deleteRowInserted(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = TestUtils.createStatement(con);){
            int rowBefore = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            String request = "DELETE FROM patient WHERE pat_id IN (SELECT pat_id FROM patient ORDER BY pat_id DESC LIMIT "+numberOfRequest+")";
            stmt.execute(request);
            int rowAfter = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertEquals("Number of row after delete shall be equals to "+(rowBefore - numberOfRequest), (rowBefore - numberOfRequest), rowAfter);
            TestUtils.closeAll(stmt, con);
        }
    }

    /**
     * method which process a scenario :
     * - Parse X INSERT request
     * - Bind X INSERT request
     * - Execute X INSERT request
     * - Parse X SELECT request
     * - Bind X SELECT request
     * - Execute X SELECT request
     * - Parse X UPDATE request
     * - Bind X UPDATE request
     * - Execute X UPDATE request
     * - Parse X DELETE request
     * - Bind X DELETE request
     * - Execute X DELETE request
     * @param numberOfRequet
     * @throws SQLException
     */
    private void parseBindExecuteLogicScenario(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = TestUtils.createStatement(con);){
            // Check number of row before scenario
            int rowBeforeInsert = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Table patient shall be not empty", rowBeforeInsert);
            // Parse X INSERT request
            List<PreparedStatement> lstInsertParsed = TestUtils.generateInsertPreparedStatementRequest(numberOfRequest);
            parseRequestSuccessfullTest(numberOfRequest, lstInsertParsed);
            // Bind X INSERT request
            List<PreparedStatement> lstInsertBinded = TestUtils.bindInsertRequest(numberOfRequest, lstInsertParsed);
            bindRequestSuccessfullTest(numberOfRequest, lstInsertBinded);
            // Execute X INSERT request
            int counter = 1;
            Iterator<PreparedStatement> iterInsert = lstInsertBinded.iterator();
            while(iterInsert.hasNext() && counter <= numberOfRequest){
                stmt.execute(String.valueOf(iterInsert.next()));
                Assert.assertEquals("Table patient shall contains "+(rowBeforeInsert + counter)+" rows", (rowBeforeInsert + counter), TestUtils.getNumberOfRow(stmt, "patient", "pat_id"));
                counter ++;
            }
            counter = 0;
            // check number of row after insert
            int rowAfterInsert = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertNotNull("Table patient shall be not empty", rowAfterInsert);
            Assert.assertEquals("Insert result's request unsuccesfull", rowBeforeInsert + numberOfRequest, rowAfterInsert);
            // Parse X SELECT request
            List<PreparedStatement> lstSelectParsed = TestUtils.generateSelectPreparedStatementRequest(numberOfRequest);
            parseRequestSuccessfullTest(numberOfRequest, lstSelectParsed);
            // Bind X SELECT request
            List<PreparedStatement> lstSelectBinded = new ArrayList<PreparedStatement>();
            List<String> lstIdSelected = new ArrayList<String>();
            String request = "SELECT pat_id FROM patient WHERE pat_id IN (SELECT pat_id FROM patient ORDER BY pat_id DESC LIMIT "+numberOfRequest+") ORDER BY pat_id";
            ResultSet res = stmt.executeQuery(request);
            while(res.next()){
                String id = res.getString(1);
                lstIdSelected.add(id);
            }
            for(String id : lstIdSelected){
                PreparedStatement prep = TestUtils.bindSelectRequest(numberOfRequest, lstSelectParsed.get(counter), id);
                lstSelectBinded.add(prep);
                counter ++;
            }
            counter = 1;
            bindRequestSuccessfullTest(numberOfRequest, lstSelectBinded);
            // Execute X Select request
            Iterator<PreparedStatement> iterSelect = lstSelectBinded.iterator();
            while(iterSelect.hasNext() && counter < numberOfRequest){
                stmt.execute(String.valueOf(lstSelectBinded.get(counter)));
                counter ++;
            }
            counter = 1;
            // Parse X UPDATE request
            List<PreparedStatement> lstUpdateParsed = TestUtils.generateUpdatePreparedStatementRequest(numberOfRequest);
            parseRequestSuccessfullTest(numberOfRequest, lstUpdateParsed);
            // Bind X UPDATE request
            List<PreparedStatement> lstUpdateBinded = TestUtils.bindUpdateRequest(numberOfRequest, lstUpdateParsed, lstIdSelected);
            bindRequestSuccessfullTest(numberOfRequest, lstUpdateBinded);
            // Execute X UPDATE request
            Iterator<PreparedStatement> iterUpdate = lstUpdateBinded.iterator();
            while(iterUpdate.hasNext() && counter < numberOfRequest){
                PreparedStatement prepStmt = iterUpdate.next();
                stmt.execute(String.valueOf(prepStmt));
                String rowBeforUpdate = getRowBeforeUpdate(con, prepStmt);
                updateRequestTestSuccesfull(con, prepStmt, rowBeforUpdate);
                updateRequestTestSuccesfull(con, prepStmt, rowBeforUpdate);
            }
            counter ++;
            // Parse DELETE request
            PreparedStatement requestDeleteParsed = TestUtils.generateDeletePreparedStatementRequest(numberOfRequest);
            // Bind DELETE request
            PreparedStatement requestDeleteBinded = TestUtils.bindDeleteBetweenRequest(numberOfRequest, requestDeleteParsed, lstIdSelected.get(0), lstIdSelected.get(lstIdSelected.size() - 1));
            // Execute DELETE request
            stmt.execute(String.valueOf(requestDeleteBinded));
            int rowEnd = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertEquals("Table patient shall contains "+(rowAfterInsert - numberOfRequest)+" rows", (rowAfterInsert - numberOfRequest), rowEnd);
            TestUtils.closeAll(stmt, con);
        }
    }

    /**
     * method which process a scenario :
     * - INSERT X row in table patient
     * - Generate and parse 2X INSERT request
     * - Generate and parse X UPDATE request
     * - Bind Y (random) UPDATE request
     * - Execute Y DELETE request
     * - Bind X INSERT parsed request (not all request parsed)
     * - Execute X binded INSERT request
     * - Bind remaining INSERT parsed request (never executed)
     * - Parse and bind a DELETE request which delete all previous row inserted
     * - Execute DELETE request
     * All these actions take a test
     *
     * @param numberOfRequest
     * @throws SQLException
     */
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
                stmt.execute(String.valueOf(prepStmt));
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
            int rowEnd = TestUtils.getNumberOfRow(stmt, "patient", "pat_id");
            Assert.assertEquals("Table patient shall contains "+(rowAfter + numberOfRequest - (numberOfRequest*2))+" rows", (rowAfter + numberOfRequest - (numberOfRequest*2)), rowEnd);
            TestUtils.closeAll(stmt, con);
        }
    }

    /**
     * method which checks if UPDATE request process successfully
     * @param con
     * @param prepStmt
     * @param requestBefore
     * @throws SQLException
     */
    private void updateRequestTestSuccesfull(Connection con, PreparedStatement prepStmt, String requestBefore) throws SQLException{
        String request = String.valueOf(prepStmt);
        int index = request.indexOf("pat_id") + 10;
        String id = request.substring(index, index + 8);
        Statement stmt = TestUtils.createStatement(con);
        String req = "SELECT * FROM patient WHERE pat_id = '"+id+"'";
        ResultSet res = stmt.executeQuery(req);
        Assert.assertNotEquals("Row update unsuccesfull", String.valueOf(res), requestBefore);
    }


    /**
     * method which return value of Id's patient in order to check if UPDATE request process successfully
     * @param con
     * @param prepStmt
     * @return
     * @throws SQLException
     */
    private String getRowBeforeUpdate(Connection con, PreparedStatement prepStmt) throws SQLException{
        String request = String.valueOf(prepStmt);
        int index = request.indexOf("pat_id") + 10;
        String id = request.substring(index, index + 8);
        Statement stmt = TestUtils.createStatement(con);
        String req = "SELECT * FROM patient WHERE pat_id = '"+id+"'";
        ResultSet res = stmt.executeQuery(req);
        return String.valueOf(res);
    }

}
