package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;

/**
 * @author Mehdi.BENANIBA
 *
 */
public class TestUtils {

    private TestUtils(){
    }
    
    /**
     * method which assure connection to health database
     * @return
     * @throws SQLException
     */
    public static Connection getHealthConnection() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", "postgres");
        connectionProps.put("password", "postgres");
        return DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/ehealth",
                connectionProps);
    }
    
    /**
     * method which close statement and connection enter as parameter
     * @param stmt
     * @param connection
     * @throws SQLException
     */
    public static void closeAll(Statement stmt, Connection connection) throws SQLException{
        stmt.close();
        connection.close();
    }
    
    /**
     * method which return number of simultaneous connection
     * @return
     * @throws SQLException
     */
    public static int getNumberOfConnection() throws SQLException{
        int numberOfConnection = 0;
        try(Connection con = getHealthConnection(); Statement stmt = createStatement(con);){
            String request = "SELECT count(*) FROM pg_catalog.pg_stat_activity";
            ResultSet res = stmt.executeQuery(request);
            res.first();
            numberOfConnection = res.getInt(1);
        }
        return numberOfConnection;
    }
    
    /**
     * method which check how many connection simultaneous could be handle
     * @return
     * @throws SQLException
     */
    public static int getMaxNumberConnectionSimultaneous() throws SQLException{
        int numberOfConnection = getNumberOfConnection();
        Assert.assertNotNull(numberOfConnection);
        List<Connection> lstCon = new ArrayList<Connection>();
        try{
            while(true){
                Connection con = TestUtils.getHealthConnection();
                lstCon.add(con);
            }
        }catch(Exception e){
            
        }finally{
            numberOfConnection = numberOfConnection + lstCon.size();
            Iterator<Connection> i = lstCon.iterator();
            while(i.hasNext()){
                i.next().close();
            }
        }
        return numberOfConnection;
    }
    
    /**
     * method which allow to retrieve patient's id as string when request is processed
     * @param number
     * @return
     */
    public static String concatenationZeroIntAsString(int number, int length){
            String idrow = String.valueOf(number);
            int rowLength = idrow.length();
            if(rowLength < length){
                String concat = "0";
                while(concat.length() < (length - rowLength)){
                    concat = concat.concat("0");
                }
                idrow = concat.concat(idrow);
            }
            return idrow;
        }
    
    /**
     * method which allow to retrieve field's value (String) from request result. Specify column is necessary
     * @param res
     * @param column
     * @param rowPosition
     * @return
     * @throws SQLException
     */
    public static String getFirstOrLastFieldValueString(ResultSet res, int column, String rowPosition) throws SQLException{
        String fieldValue = null;
        switch (rowPosition){
        case "first" : 
            res.first();
            break;
        case "last" : 
            res.last();
            break;
        }
        fieldValue = res.getString(column);
        return fieldValue;
    }
    
    /**
     * method which allow to retrieve field's value (int) from request result. Specify column is necessary
     * @param res
     * @param column
     * @param rowPosition
     * @return
     * @throws SQLException
     */
    public static int getFirstOrLastFieldValueInt(ResultSet res, int column, String rowPosition) throws SQLException{
        int fieldValue = 0;
        switch (rowPosition){
        case "first" : 
            res.first();
            break;
        case "last" : 
            res.last();
            break;
        }
        fieldValue = Integer.parseInt(res.getString(column));
        return fieldValue;
    }
    
    /**
     * method which allow to retrieve field's value (Date) from request result. Specify column is necessary
     * @param res
     * @param column
     * @param rowPosition
     * @return
     * @throws SQLException
     */
    public static Date getFirstOrLastFieldValueDate(ResultSet res, int column, String rowPosition) throws SQLException{
        Date fieldValue = null;
        switch (rowPosition){
        case "first" : 
            res.first();
            break;
        case "last" : 
            res.last();
            break;
        }
        fieldValue = res.getDate(column);
        return fieldValue;
    }
    
    /**
     * method which return number of cursor's row
     * @param res
     * @return
     * @throws SQLException
     */
    public static int getRowCount(ResultSet res) throws SQLException{
        res.last();
        int rowNumber = res.getRow();
        return rowNumber;
    }
    
    /**
     * method which create a new patient's Id by incrementing the last one
     * @param res
     * @return
     * @throws SQLException
     */
    public static String createNewIdByIncrement(ResultSet res) throws SQLException{
        res.last();
        String idStringValue = getFirstOrLastFieldValueString(res, 1, "last");
        int idNewIntValue = Integer.parseInt(idStringValue) + 1;
        String newIdIncremented = concatenationZeroIntAsString(idNewIntValue, 8);       
        return newIdIncremented;
    }
    
    /**
     * method which generate random String upper case as long as (int) parameter 
     * @param length
     * @return
     */
    public static String generateRandomString(){
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String pass = "";
        int counter = 0;
        int length = generateRandomInt(5, 10);
        while(counter < length)
        {
           int i = (int)Math.floor(Math.random() * 26); 
           pass += chars.charAt(i);
           counter = counter + 1;
        }
        return pass;
    }
    
    /**
     * method which generate a random (int) variable between min/max parameter
     * @param min
     * @param max
     * @return
     */
    public static int generateRandomInt(int min, int max){
        int integer = (int) ((max-min)*Math.random()) + min;
        return integer;
    }
    
    /**
     * method which allow to convert a Date to String
     * @param date
     * @return
     */
    public static String convertDateToString(Date date){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = dateFormat.format(date); 
        return dateString;
    }
    
    /**
     * method which allow to instantiate a new Date from a String value
     * @param stringDate
     * @return
     * @throws ParseException
     */
    public static java.sql.Date instantiateNewDate(String stringDate) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date parsed = sdf.parse(stringDate);
        java.sql.Date date = new java.sql.Date(parsed.getTime());
        return date;
    }
    
    /**
     * method which create a new Statement which could be scrolled 
     * @param con
     * @return
     * @throws SQLException
     */
    public static Statement createStatement(Connection con) throws SQLException{
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_READ_ONLY, 
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        return stmt;
    }
    
    /**
     * method which returns number of row of table define in parameter
     * @param stmt
     * @param tableName
     * @param orderBy
     * @return
     * @throws SQLException
     */
    public static int getNumberOfRow(Statement stmt, String tableName, String orderBy) throws SQLException{
        String rqstCountRow = "SELECT * FROM patient "+tableName+" ORDER BY "+orderBy+"";
        ResultSet resCheck = stmt.executeQuery(rqstCountRow);
        int row = getRowCount(resCheck);
        return row;
    }
    
    public static List<String> generateInsertRequest(int numberOfRequest) throws SQLException{
        try(Connection con = getHealthConnection(); Statement stmt = createStatement(con);){
            List<String> tabRequest = new ArrayList<String>();
            int counter = 0;
            String rqstSelect = "SELECT * FROM patient ORDER BY pat_id"; //initialize SELECT request
            ResultSet resRequest = stmt.executeQuery(rqstSelect); //execute SELECT
            String newId = createNewIdByIncrement(resRequest); //initialize first new id
            while(counter < numberOfRequest){
                //generate new name, last name and last name 2
                String newName = generateRandomString(); //generate random new name
                String newLast = generateRandomString(); //generate random new last name
                String newLast2 = generateRandomString(); //generate random new last name 2
                //initialize INSERT request
                String request = "INSERT INTO PATIENT VALUES ('"+newId+"', '"+newName+"', '"+newLast+"', '"+newLast2+"', 'M', 'OTHER')";
                tabRequest.add(request);
                counter = counter + 1; //increment counter
                int newIdAsInt = Integer.parseInt(newId) + 1; //increment id
                newId = concatenationZeroIntAsString(newIdAsInt, 8);
            }
            return tabRequest;
        }
    }
    
    /**
     * method which generate INSERT request (ready to insert) with X row values
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static String generateInsertAllRequest(int numberOfRequest) throws SQLException{
        try(Connection con = getHealthConnection(); Statement stmt = createStatement(con);){
            int counter = 0;
            String rqstSelect = "SELECT * FROM patient ORDER BY pat_id"; //initialize SELECT request
            ResultSet resRequest = stmt.executeQuery(rqstSelect); //execute SELECT
            String newId = createNewIdByIncrement(resRequest); //initialize first new id
            String request = "INSERT INTO PATIENT (pat_id, pat_name, pat_last1, pat_last2, pat_gen, pat_zip) VALUES ";
            //initialize INSERT request values
            while(counter < numberOfRequest){
                String newName = generateRandomString(); //generate random new name
                String newLast = generateRandomString(); //generate random new last name
                String newLast2 = generateRandomString(); //generate random new last name 2
                if(counter != numberOfRequest - 1){
                    request = request + "('"+newId+"', '"+newName+"', '"+newLast+"', '"+newLast2+"', 'M', 'OTHER'), ";
                }
                else{
                    request = request + "('"+newId+"', '"+newName+"', '"+newLast+"', '"+newLast2+"', 'M', 'OTHER');";
                }
                counter = counter + 1; //increment counter
                int newIdAsInt = Integer.parseInt(newId) + 1; //increment id
                newId = concatenationZeroIntAsString(newIdAsInt, 8);
            }
            return request;
        }
    }
    
    /**
     * method which generate a list of X prepared statement containing INSERT request for PATIENT table
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> generateInsertPreparedStatementRequest(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            List<PreparedStatement> lstPreparedStmt = new ArrayList<PreparedStatement>();
            String request = "INSERT INTO PATIENT VALUES (?, ?, ?, ?, 'M', 'OTHER')";
            int counter = 0;
            while(counter < numberOfRequest){
                PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                lstPreparedStmt.add(prep);
                counter = counter + 1; //increment counter
            }
            return lstPreparedStmt;
        }
    }
    
    
    /**
     * method which generate a list of X prepared statement containing SELECT request for PATIENT table
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> generateSelectPreparedStatementRequest(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            List<PreparedStatement> lstPreparedStmt = new ArrayList<PreparedStatement>();
            String request = "SELECT * FROM PATIENT WHERE pat_id = ?";
            int counter = 0;
            while(counter < numberOfRequest){
                PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                lstPreparedStmt.add(prep);
                counter = counter + 1; //increment counter
            }
            return lstPreparedStmt;
        }
    }
    
    /**
     * method which generate a list of X prepared statement containing UPDATE request for PATIENT table
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> generateUpdatePreparedStatementRequest(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            List<PreparedStatement> lstPreparedStmt = new ArrayList<PreparedStatement>();
            String request = "UPDATE PATIENT SET pat_name = ?, pat_last1 = ?, pat_last2 = ?, pat_gen = 'M', pat_zip = 'OTHER' WHERE pat_id = ?";
            int counter = 0;
            while(counter < numberOfRequest){
                PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                lstPreparedStmt.add(prep);
                counter = counter + 1; //increment counter
            }
            return lstPreparedStmt;
        }
    }
    
    /**
     * method which bind SELECT request content of preparedStatement parameter
     * @param numberOfRequest
     * @param lstPrepStmt
     * @param idToSelect
     * @return
     * @throws SQLException
     */
    public static PreparedStatement bindSelectRequest(int numberOfRequest, PreparedStatement prepStmt, String idToSelect) throws SQLException{
        prepStmt.setString(1, idToSelect);
        return prepStmt;
        }
    
    /**
     * method which bind INSERT request content of list of preparedStatement parameter
     * @param numberOfRequest
     * @param lstPrepStmt
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> bindInsertRequest(int numberOfRequest, List<PreparedStatement> lstPrepStmt) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            List<PreparedStatement> lstPreparedStmt = lstPrepStmt;
            List<PreparedStatement> lstPreparedStmtBinded = new ArrayList<PreparedStatement>();
            int counter = 0;
            String rqstSelect = "SELECT * FROM patient ORDER BY pat_id"; //initialize SELECT request
            ResultSet resRequest = stmt.executeQuery(rqstSelect); //execute SELECT
            String newId = createNewIdByIncrement(resRequest); //initialize first new id
            while(counter < numberOfRequest){
                String newName = generateRandomString(); //generate random new name
                String newLast = generateRandomString(); //generate random new last name
                String newLast2 = generateRandomString(); //generate random new last name 2
                PreparedStatement prep = con.prepareStatement(String.valueOf(lstPreparedStmt.get(counter)), ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                prep.setString(1, newId);
                prep.setString(2, newName);
                prep.setString(3, newLast);
                prep.setString(4, newLast2);
                lstPreparedStmtBinded.add(prep);
                counter = counter + 1; //increment counter
                int newIdAsInt = Integer.parseInt(newId) + 1; //increment id
                newId = concatenationZeroIntAsString(newIdAsInt, 8);
            }
            return lstPreparedStmtBinded;
        }
    }
    
    /**
     * method which bind UPDATE request content of list of preparedStatement parameter
     * @param numberOfRequest
     * @param lstPrepStmt
     * @param lstId
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> bindUpdateRequest(int numberOfRequest, List<PreparedStatement> lstPrepStmt, List<String> lstId) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
                List<PreparedStatement> lstPreparedStmtBinded = new ArrayList<PreparedStatement>();
                int counter = 0;
                while(counter < numberOfRequest){
                    String newName = generateRandomString(); //generate random new name
                    String newLast = generateRandomString(); //generate random new last name
                    String newLast2 = generateRandomString(); //generate random new last name 2
                    PreparedStatement prep = con.prepareStatement(String.valueOf(lstPrepStmt.get(counter)), ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_READ_ONLY,
                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    prep.setString(1, newName);
                    prep.setString(2, newLast);
                    prep.setString(3, newLast2);
                    prep.setString(4, lstId.get(counter));
                    lstPreparedStmtBinded.add(prep);
                    counter = counter + 1; //increment counter
                }
            return lstPreparedStmtBinded;
            }
        }
    
    /**
     * method which generate INSERT request already binded for table patient. Field's value are generated randomly
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> generateInsertBindRequest(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            List<PreparedStatement> lstPreparedStmt = generateInsertPreparedStatementRequest(numberOfRequest);
            List<PreparedStatement> lstPreparedStmtBinded = new ArrayList<PreparedStatement>();
            int counter = 0;
            String rqstSelect = "SELECT * FROM patient ORDER BY pat_id"; //initialize SELECT request
            ResultSet resRequest = stmt.executeQuery(rqstSelect); //execute SELECT
            String newId = createNewIdByIncrement(resRequest); //initialize first new id
            while(counter < numberOfRequest){
                String newName = generateRandomString(); //generate random new name
                String newLast = generateRandomString(); //generate random new last name
                String newLast2 = generateRandomString(); //generate random new last name 2
                PreparedStatement prep = con.prepareStatement(String.valueOf(lstPreparedStmt.get(counter)), ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                prep.setString(1, newId);
                prep.setString(2, newName);
                prep.setString(3, newLast);
                prep.setString(4, newLast2);
                lstPreparedStmtBinded.add(prep);
                counter = counter + 1; //increment counter
                int newIdAsInt = Integer.parseInt(newId) + 1; //increment id
                newId = concatenationZeroIntAsString(newIdAsInt, 8);
            }
            return lstPreparedStmtBinded;
        }
    }
    
    /**
     * method which generate SELECT request already binded for patient's Id in parameter
     * @param numberOfRequest
     * @param idToSelect
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> generateSelectBindRequest(int numberOfRequest, String idToSelect) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            List<PreparedStatement> lstPreparedStmt = generateSelectPreparedStatementRequest(numberOfRequest);
            List<PreparedStatement> lstPreparedStmtBinded = new ArrayList<PreparedStatement>();
            int counter = 0;
            while(counter < numberOfRequest){
                PreparedStatement prep = con.prepareStatement(String.valueOf(lstPreparedStmt.get(counter)), ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                prep.setString(1, idToSelect);
                lstPreparedStmtBinded.add(prep);
                counter = counter + 1; //increment counter
            }
            return lstPreparedStmtBinded;
        }
    }
    
    /**
     * method which generate UPDATE request already binded for table patient. Field's value are generated randomly
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static List<PreparedStatement> generateUpdateBindRequest(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            List<PreparedStatement> lstPreparedStmt = generateUpdatePreparedStatementRequest(numberOfRequest);
            List<PreparedStatement> lstPreparedStmtBinded = new ArrayList<PreparedStatement>();
            int counter = 0;
            while(counter < numberOfRequest){
                String newName = generateRandomString(); //generate random new name
                String newLast = generateRandomString(); //generate random new last name
                String newLast2 = generateRandomString(); //generate random new last name 2
                PreparedStatement prep = con.prepareStatement(String.valueOf(lstPreparedStmt.get(counter)), ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
                prep.setString(1, newName);
                prep.setString(2, newLast);
                prep.setString(3, newLast2);
                lstPreparedStmtBinded.add(prep);
                counter = counter + 1; //increment counter
            }
            return lstPreparedStmtBinded;
        }
    }
    
    /**
     * method which generate a prepared statement containing DELETE (BETWEEN operator) request for PATIENT table
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static PreparedStatement generateDeletePreparedStatementRequest(int numberOfRequest) throws SQLException{
        try(Connection con = TestUtils.getHealthConnection(); Statement stmt = createStatement(con);){
            String request = "DELETE FROM PATIENT WHERE pat_id BETWEEN ? AND ?";
            PreparedStatement prep = con.prepareStatement(request, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
            return prep;
        }
    }
    
    /**
     * method which generate DELETE request already binded for table patient. Field's value are generated randomly
     * @param numberOfRequest
     * @return
     * @throws SQLException
     */
    public static PreparedStatement bindDeleteBetweenRequest(int numberOfRequest, PreparedStatement prepStmt, String id1, String id2) throws SQLException{
        PreparedStatement prepDeleteBinded = prepStmt;
        prepDeleteBinded.setString(1, id1);
        prepDeleteBinded.setString(2, id2);
        return prepDeleteBinded;
    }
    
}
