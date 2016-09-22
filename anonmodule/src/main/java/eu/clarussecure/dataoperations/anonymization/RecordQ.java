package eu.clarussecure.dataoperations.anonymization;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class RecordQ {
	static int numAttr;
	static ArrayList<String>listDataTypes;
	static ArrayList<String>listAttrTypes;
	String attrValues[];
	int id;
	
	public RecordQ(int id){
		attrValues = new String[numAttr];
		this.id = id;
	}
	
	public Record toRecord(Record r){
		Record record;
		int pos;
		String attrType, dataType;
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
		
		record = r.clone();
		pos = 0;
		for(int j=0; j<Record.numAttr; j++){
			attrType = Record.listAttrTypes.get(j);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				dataType = Record.listDataTypes.get(j);
				if(dataType.equalsIgnoreCase(Constants.date)){	//epoch time a fecha
					record.attrValues[j] = format1.format(new Date(Long.parseLong(this.attrValues[pos])));
				}
				else{
					record.attrValues[j] = this.attrValues[pos];
				}
				pos++;
			}
			if(attrType.equalsIgnoreCase(Constants.identifier)){
				record.attrValues[j] = "*";
			}
		}
		return record;
	}

}
