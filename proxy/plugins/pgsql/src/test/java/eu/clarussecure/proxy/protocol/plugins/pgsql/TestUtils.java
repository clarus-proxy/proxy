package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class TestUtils {

    private TestUtils(){
    }
    
    /**
     * method which assure connection to database
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
    public static String generateRandomString(int length){
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String pass = "";
        int counter = 0;
        while(counter < length)
        {
           int i = (int)Math.floor(Math.random() * 26); 
           pass += chars.charAt(i);
           counter = counter + 1;
        }
        return pass;
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
    
}
