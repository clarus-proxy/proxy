package eu.clarussecure.dataoperations.anonymization;

import java.util.ArrayList;
import java.util.HashMap;

public class Cluster {
	private ArrayList<RecordQ>elements;
	private RecordQ centroid;

	public Cluster() {
		this.elements = new ArrayList<>();
		this.centroid = null;
	}
	
	public void add(RecordQ reg){
		this.elements.add(reg);
	}
	
	public void clear(){
		this.elements.clear();
	}
	
	public ArrayList<RecordQ> getElements(){
		return this.elements;
	}

	public int getNumReg() {
		return elements.size();
	}

	public RecordQ getCentroid(){
		if(centroid == null){
			calculateCentroid();
		}
		return centroid;
	}
	
	public void calculateCentroid(){
		long media;
		String moda;
		String dataType, attrType;
		
		
		centroid = new RecordQ(0);
		for(int i=0; i<RecordQ.numAttr; i++){
			attrType = RecordQ.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				dataType = RecordQ.listDataTypes.get(i);
				if(dataType.equalsIgnoreCase(Constants.numericDiscrete) ||
				   dataType.equalsIgnoreCase(Constants.date)){
					media = calculateMedia(i);
					centroid.attrValues[i] = String.valueOf(media);
				}
				else{
					if(dataType.equalsIgnoreCase(Constants.numericContinuous)){
						media = calculateMediaDouble(i);
						centroid.attrValues[i] = String.valueOf(media);
					}
					else{
						moda = calculateModa(i);
						centroid.attrValues[i] = moda;
					}
				}
			}
		}
	}
	
	private long calculateMedia(int attr){
		long media;
		String valor;
		
		media = 0;
		for(RecordQ reg:elements){
			valor = reg.attrValues[attr];
			media += Long.parseLong(valor);
		}
		media /= elements.size();
		
		return media;
	}
	
	private long calculateMediaDouble(int attr){
		double media;
		String valor;
		
		media = 0;
		for(RecordQ reg:elements){
			valor = reg.attrValues[attr];
			media += Long.parseLong(valor);
		}
		media /= elements.size();
		
		return (long)media;
	}
	
	private String calculateModa(int attr){
		String moda, valor;
		Integer v, maxV;
		HashMap<String,Integer>control = new HashMap<String,Integer>();
		
		for(RecordQ reg:elements){
			valor = reg.attrValues[attr];
			v = control.get(valor);
			if(v == null){
				control.put(valor, 1);
			}
			else{
				v++;
				control.put(valor, v);
			}
		}
		maxV = 0;
		moda = "";
		for(String s:control.keySet()){
			v = control.get(s);
			if(v > maxV){
				moda = s;
				maxV = v;
			}
		}
		return moda;
	}
}