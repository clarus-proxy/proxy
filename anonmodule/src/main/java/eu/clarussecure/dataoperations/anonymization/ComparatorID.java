package eu.clarussecure.dataoperations.anonymization;

import java.util.Comparator;

public class ComparatorID implements Comparator<RecordQ>{
	
	public int compare(RecordQ o1, RecordQ o2) {
		return o1.id - o2.id;
	}


}
