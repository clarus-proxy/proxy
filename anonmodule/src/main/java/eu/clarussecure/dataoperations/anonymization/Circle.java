package eu.clarussecure.dataoperations.anonymization;

public class Circle {
	CoordinateS centre;
	double radius;
	
	public Circle(CoordinateS centre, double radius) {
		this.centre = new CoordinateS(centre.latitude, centre.longitude);
		this.radius = radius;
	}
	public Circle(double x, double y, double radius) {
		this.centre = new CoordinateS(y, x);
		this.radius = radius;
	}
	public Circle(double x, double y) {
		this.centre = new CoordinateS(x,y);
	}
	public CoordinateS getCentre() {
		return centre;
	}
	public double getRadius() {
		return radius;
	}

}
