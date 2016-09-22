package eu.clarussecure.dataoperations.anonymization;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

public class Record {
	static String attributeSeparator;
	static String recordSeparator;
	static boolean header;
	static int numAttr;
	static int numQuasi;
	static HashMap<String,String>attrTypes;
	static int k;
	static double t;
	static int clouds;
	static String idKey;
	static String coarsening_type;
        //AKKA fix: radius value depends on SRID. it could be a real
	//static int radius;
        static double radius;
	static ArrayList<String>listNames;
	static ArrayList<String>listAttrTypes;
	static ArrayList<String>listDataTypes;
	String attrValues[];
	int id;

	public Record(int id) {
		this.attrValues = new String[numAttr];
		this.id = id;
	}
	
	public Record(int id, int numAttr) {	//per num attr diferent
		this.attrValues = new String[numAttr];
		this.id = id;
	}
	
	public RecordQ toRecordQ(){
		RecordQ recordQ;
		int pos;
		String attrType, dataType, value;
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
		
		recordQ = new RecordQ(this.id);
		pos = 0;
		for(int i=0; i<listAttrTypes.size(); i++){
			attrType = listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				dataType = Record.listDataTypes.get(i);
				if(dataType.equalsIgnoreCase(Constants.date)){	//fechas a epoch time
					value = this.attrValues[i];
					try {
						calendar.setTime(format1.parse(value));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					recordQ.attrValues[pos] = String.valueOf(calendar.getTimeInMillis());
				}
				else{	//resto igual
					recordQ.attrValues[pos] = this.attrValues[i];
				}
				pos++;
			}
		}
		return recordQ;
	}
	
	public RecordQ toRecordQConfidential(){
		RecordQ recordQ;
		int pos;
		String attrType, dataType, value;
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
		
		recordQ = new RecordQ(this.id);
		pos = 0;
		for(int i=0; i<listAttrTypes.size(); i++){
			attrType = listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				dataType = Record.listDataTypes.get(i);
				if(dataType.equalsIgnoreCase(Constants.date)){	//fechas a epoch time
					value = this.attrValues[i];
					try {
						calendar.setTime(format1.parse(value));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					recordQ.attrValues[pos] = String.valueOf(calendar.getTimeInMillis());
				}
				else{	//resto igual
					recordQ.attrValues[pos] = this.attrValues[i];
				}
				pos++;
			}
			
		}
		for(int i=0; i<listAttrTypes.size(); i++){
			attrType = listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.confidential)){
				recordQ.attrValues[pos] = this.attrValues[i];
				break;	// only 1 confidential
			}
		}
		
		return recordQ;
	}
	
	public Record clone(){
		Record record;
		
		record = new Record(this.id);
		for(int i=0; i<this.attrValues.length; i++){
			record.attrValues[i] = this.attrValues[i];
		}
		return record;
	}
	
	public String toString(){
		String str;
		str = "";
		for(String s:attrValues){
			str += s + attributeSeparator;
		}
		
		if(str.equals(""))return "";
		return str.substring(0, str.length()-1) + recordSeparator;
	}
	
	public String[] toVectorString(){
		String str[];
		
		str = new String[numAttr];
		for(int i=0; i<numAttr; i++){
			str[i] = attrValues[i];
		}
		
		return str;
	}
	
	public StringBuilder toStringBuilder(){
		StringBuilder str;
		
		str = new StringBuilder("");
		for(String s:attrValues){
			str.append(s).append(attributeSeparator);
		}
		
		if(str.equals(""))return new StringBuilder("");
		return new StringBuilder(str.substring(0, str.length()-1)).append(recordSeparator);
	}
	
}
