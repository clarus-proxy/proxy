package eu.clarussecure.dataoperations.anonymization;

import java.util.ArrayList;

public class ClusterPoints {
	private ArrayList<CoordinateS>points;
	private CoordinateS centroid;

	public ClusterPoints() {
		this.points = new ArrayList<>();
		this.centroid = null;
	}
	
	public void add(CoordinateS point){
		this.points.add(point);
	}
	
	public void clear(){
		this.points.clear();
	}
	
	public ArrayList<CoordinateS> getPoints(){
		return this.points;
	}

	public int getNumPoints() {
		return points.size();
	}

	public CoordinateS getCentroid(){
		if(centroid == null){
			calculateCentroid();
		}
		return centroid;
	}
	
	public CoordinateS calculateCentroid(){
		double maxX, maxY, minX, minY;
		double x, y;
		
		maxX = maxY = -Double.MAX_VALUE;
		minX = minY = Double.MAX_VALUE;
		centroid = new CoordinateS(maxX, maxY);
		for(CoordinateS p:points){
			x = p.latitude;
			y = p.longitude;
			if(x > maxX){
				maxX = x;
			}
			if(y > maxY){
				maxY = y;
			}
			if(x < minX){
				minX = x;
			}
			if(y < minY){
				minY = y;
			}
		}
		x = (maxX + minX) / 2;
		y = (maxY + minY) / 2;
		centroid.setLocation(x, y);
		
		return centroid;
	}
	
	public String toString(){
		String s;
		
		s = "";
		for(CoordinateS c:points){
			s += c + "\n";
		}
		s += "c:" + centroid;
		return s;
	}
}
