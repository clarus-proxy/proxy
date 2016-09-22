package eu.clarussecure.dataoperations.anonymization;

import java.util.ArrayList;

public class Distances {
	
	static double typicalDev[];
	
	public static double euclideanDistNorm(RecordQ c1, RecordQ c2){
		double dis, partial, partial1, cn1, cn2;
		String attrType, dataType;
				
		dis = 0;
		partial = partial1 = 0;
		for(int i=0; i<RecordQ.numAttr; i++){
			attrType = RecordQ.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				dataType = RecordQ.listDataTypes.get(i);
				if(dataType.equalsIgnoreCase(Constants.numericDiscrete) || 
				   dataType.equalsIgnoreCase(Constants.numericContinuous) ||
				   dataType.equalsIgnoreCase(Constants.date)){
					cn1 = Double.parseDouble(c1.attrValues[i]);
					cn2 = Double.parseDouble(c2.attrValues[i]);
					partial1 = (cn1-cn2) / typicalDev[i];
				}
				if(dataType.equalsIgnoreCase(Constants.categoric)){
					if(c1.attrValues[i].equalsIgnoreCase(c2.attrValues[i])){
						partial1 = 0.0;
					}
					else{
						partial = 1.0;
					}
					//partial /= typicalDev[i];
				}
				partial += (partial1 * partial1);
			}
		}
		dis = Math.sqrt(partial);
		return dis;
	}
	
	public static void calculateTypicalDeviationsNumeric(ArrayList<RecordQ>data){
		double var[];
		int numAttr;
		double max[];
		double min[];
		double dev;
		
		numAttr = data.get(0).attrValues.length;
		typicalDev = new double[numAttr];
		max = new double[numAttr];
		min = new double[numAttr];
		
		for(int i=0; i<numAttr; i++){
			if(RecordQ.listDataTypes.get(i).equalsIgnoreCase(Constants.categoric) ||
			   RecordQ.listDataTypes.get(i).equalsIgnoreCase(Constants.categoricOrdinal)){
				dev = 0.5;
			}
			else{
				var = new double[data.size()];
				max[i] = Double.parseDouble(data.get(0).attrValues[i]);
				min[i] = Double.parseDouble(data.get(0).attrValues[i]);
				for(int j=0; j<data.size(); j++){
					var[j] = Double.parseDouble(data.get(j).attrValues[i]);
					if(var[j] > max[i]){
						max[i] = var[j];
					}
					if(var[j] < min[i]){
						min[i] = var[j];
					}
				}
				dev = calculateTypicalDeviation(var);
			}
			typicalDev[i] = dev;
		}
	}
	
	public static void calculateTypicalDeviationsNumericWithConfidential(ArrayList<RecordQ>data){
		double var[];
		int numAttr;
		double max[];
		double min[];
		double dev;
		
		numAttr = data.get(0).attrValues.length-1;
		typicalDev = new double[numAttr];
		max = new double[numAttr];
		min = new double[numAttr];
		
		for(int i=0; i<numAttr; i++){
			if(RecordQ.listDataTypes.get(i).equalsIgnoreCase(Constants.categoric) ||
			   RecordQ.listDataTypes.get(i).equalsIgnoreCase(Constants.categoricOrdinal)){
				dev = 0.5;
			}
			else{
				var = new double[data.size()];
				max[i] = Double.parseDouble(data.get(0).attrValues[i]);
				min[i] = Double.parseDouble(data.get(0).attrValues[i]);
				for(int j=0; j<data.size(); j++){
					var[j] = Double.parseDouble(data.get(j).attrValues[i]);
					if(var[j] > max[i]){
						max[i] = var[j];
					}
					if(var[j] < min[i]){
						min[i] = var[j];
					}
				}
				dev = calculateTypicalDeviation(var);
			}
			typicalDev[i] = dev;
		}
	}
	
	private static double calculateTypicalDeviation(double var[]){
		double tipicalDev, medianVar, partial;
		
		medianVar = 0;
		for(int i=0; i<var.length; i++){
			medianVar += var[i];
		}
		medianVar /= var.length;
		
		tipicalDev = 0;
		for(int i=0; i<var.length; i++){
			partial = var[i] - medianVar;
			partial = partial * partial;
			tipicalDev += partial;
		}
		tipicalDev /= (var.length - 1);
		tipicalDev = Math.sqrt(tipicalDev);
		
		return tipicalDev;
	}
}
