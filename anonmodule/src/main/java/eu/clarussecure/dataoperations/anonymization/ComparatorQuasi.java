package eu.clarussecure.dataoperations.anonymization;

import java.util.Comparator;

public class ComparatorQuasi implements Comparator<RecordQ>{
	
	static RecordQ zero;
	
	public static void setAttributeSortCriteria(RecordQ record){
		String dataType, attrType;
		
		zero = new RecordQ(0);
		for(int i=0; i<RecordQ.numAttr; i++){
			attrType = RecordQ.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				dataType = RecordQ.listDataTypes.get(i);
				if(dataType.equalsIgnoreCase(Constants.numericDiscrete) || 
				   dataType.equalsIgnoreCase(Constants.numericContinuous) ||
				   dataType.equalsIgnoreCase(Constants.date)){
					zero.attrValues[i] = "0";
				}
				if(dataType.equalsIgnoreCase(Constants.categoric) || dataType.equalsIgnoreCase(Constants.categoricOrdinal)){
					zero.attrValues[i] = record.attrValues[i];
				}
			}
		}
	}
	
	public int compare(RecordQ o1, RecordQ o2) {
		
		double dis1 = Distances.euclideanDistNorm(o1, zero);
		double dis2 = Distances.euclideanDistNorm(o2, zero);
		
		if(dis1 > dis2){
			return 1;
		}
		if(dis1 < dis2){
			return -1;
		}
		
		return 0;
	}

}
