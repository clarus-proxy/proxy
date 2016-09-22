package eu.clarussecure.dataoperations.anonymization;

public class CoordinateS {
	double latitude;
	double longitude;
	int id;
        //AKKA fix: SRID
	int srid;
	
	public CoordinateS(double latitude, double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	public CoordinateS(double latitude, double longitude, int id) {
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	//AKKA fix: SRID
        public CoordinateS(double latitude, double longitude, int id, int srid) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.id = id;
            this.srid = srid;
        }
    
	public void setLocation(double lat, double lon){
		this.latitude = lat;
		this.longitude = lon;
	}
	
	public double distanceSq(CoordinateS coordinate) {
        double x = coordinate.latitude - this.latitude;
        double y = coordinate.longitude - this.longitude;
        return (x * x + y * y);
    }
	
	public double distanciaHaversine(CoordinateS coordinate){
		 final double R = 6372800.0; // in meters
		 double lat1, lat2;
		 double dLat = Math.toRadians(this.latitude - coordinate.latitude);
		 double dLon = Math.toRadians(this.longitude - coordinate.longitude);
		 
		 lat1 = Math.toRadians(this.latitude);
		 lat2 = Math.toRadians(coordinate.latitude);
		 
		 double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1) * Math.cos(lat2);
		 double c = 2 * Math.asin(Math.sqrt(a));
		 return R * c;
	}
	
	public double distanciaFast(CoordinateS coordinate){
		double lat2, lon2, dist, deglen, x, y;
		
		lat2 = coordinate.latitude;
		lon2 = coordinate.longitude;
		deglen = 110.25;
		x = this.latitude - lat2;
		y = (this.longitude - lon2) * Math.cos(lat2);
		dist = deglen * Math.sqrt(x*x + y*y);
		dist *= 1000;	//in meters
		return dist;
	}
	
	@Override
	public String toString() {
		//return "lat=" + latitude + ", lon=" + longitude;
		return latitude + "," + longitude;
	}
}
