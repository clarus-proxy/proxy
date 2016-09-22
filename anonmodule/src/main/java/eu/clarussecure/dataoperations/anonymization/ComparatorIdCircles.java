package eu.clarussecure.dataoperations.anonymization;

import java.util.Comparator;

public class ComparatorIdCircles implements Comparator<Circle>{
	
	public int compare(Circle o1, Circle o2) {
		return o1.centre.id - o2.centre.id;
	}


}