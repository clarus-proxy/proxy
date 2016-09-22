package eu.clarussecure.dataoperations.anonymization;

import java.util.Comparator;

public class ComparatorSensitive implements Comparator<RecordQ>{
	static int attrSortCriteria;
	int res;
	
	public static void setAttributeSortCriteria(int attr){
		attrSortCriteria = attr;
	}
	
	public int compare(RecordQ o1, RecordQ o2) {
		String v1, v2;
		
		v1 = o1.attrValues[attrSortCriteria];
		v2 = o2.attrValues[attrSortCriteria];
		res = v1.compareTo(v2);
		return res;
	}
}
