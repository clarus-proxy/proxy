package eu.clarussecure.dataoperations.anonymization;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.geotools.geometry.jts.GeometryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

public class Functions{

    //AKKA fix: log
    private static final Logger LOGGER = LoggerFactory.getLogger(Functions.class);
	
	public static String[][] anonymize(String[] attributes, String[][] content){
		String[][] dataAnom = null;
		
		if(Record.attrTypes.get(Constants.quasiIdentifier).equalsIgnoreCase(Constants.kAnonymity) &&
		   Record.attrTypes.get("confidential").equalsIgnoreCase("t-closeness")){
			dataAnom = kAnonymize_tCloseness(content, Record.k, Record.t);
			return dataAnom;
		}

		if(Record.attrTypes.get(Constants.quasiIdentifier).equalsIgnoreCase(Constants.kAnonymity)){
			dataAnom = kAnonymize(content, Record.k);
			return dataAnom;
		}
		
		if(Record.attrTypes.get(Constants.identifier).equalsIgnoreCase(Constants.coarsening) &&
		   Record.coarsening_type.equalsIgnoreCase("shift")){
			dataAnom = coarseningShift(content, Record.radius);
			return dataAnom;
		}
		
		if(Record.attrTypes.get(Constants.identifier).equalsIgnoreCase(Constants.coarsening) &&
		   Record.coarsening_type.equalsIgnoreCase(Constants.microaggregation)){
			dataAnom = coarseningMicroaggregation(content, Record.k);
			return dataAnom;
		}

		return dataAnom;
	}
	
	/**
	 * This function applies k-anonymization to a dataset 
	 * 
	 * @param dataOri, the dataset
	 * @param k, the desired k level
	 * @return the anonymized version of the dataset that fullfils k-anonymity
	 */
	
	public static String[][] kAnonymize(String[][] dataOri, int k){
		ArrayList<Record>data;
		ArrayList<Record>dataAnom;
		String[][] dataAnomStr;
	
		data = createRecords(dataOri);
		dataAnom = kAnonymize(data, k);
		dataAnomStr = createMatrixStringFromRecords(dataAnom);
		
		return dataAnomStr;
	}

	public static ArrayList<Record> kAnonymize(ArrayList<Record>dataOri, int k){
		ArrayList<RecordQ>dataQuasis = new ArrayList<>();
		ArrayList<Record>dataAnom = new ArrayList<>();
		int pos, remain, numReg;
		Cluster cluster;
		RecordQ recordQ;
		Record record;
		String attrType;
		
                //AKKA fix: log
		//System.out.println("Anonymizing kAnonymity k = " + k + "...");
		LOGGER.trace("Anonymizing kAnonymity k = {}...", k);
		
		RecordQ.numAttr = Record.numQuasi;
		RecordQ.listAttrTypes = new ArrayList<String>();
		RecordQ.listDataTypes = new ArrayList<String>();
		for(int i=0; i<Record.numAttr; i++){
			attrType = Record.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				RecordQ.listAttrTypes.add(Record.listAttrTypes.get(i));
				RecordQ.listDataTypes.add(Record.listDataTypes.get(i));
			}
		}
		for(Record reg:dataOri){	//crea records con solo los quasi
			dataQuasis.add(reg.toRecordQ());
		}
		
		Distances.calculateTypicalDeviationsNumeric(dataQuasis);
                //AKKA fix: log
                //System.out.print("Sorting by quasi-identifiers...");
                LOGGER.trace("Sorting by quasi-identifiers...");
		Functions.sortByQuasi(dataQuasis);
                //AKKA fix: log
		//System.out.println("done");
                LOGGER.debug("Sorting by quasi-identifiers done");
		
                //AKKA fix: log
		//System.out.print("Anonymizing...");
                LOGGER.trace("Anonymizing...");
		
		cluster = new Cluster();
		numReg = dataQuasis.size();
		pos = 0;
		remain = numReg;
		while(remain >= (2*k)){
			for(int i=0; i<k; i++){
				cluster.add(dataQuasis.get(pos));
				pos++;
			}
			cluster.calculateCentroid();
			pos -= k;
			for(int i=0; i<k; i++){
				for(int j=0; j<RecordQ.numAttr; j++){
					dataQuasis.get(pos).attrValues[j] = cluster.getCentroid().attrValues[j];
				}
				pos++;
			}
			cluster.clear();
			remain = numReg - pos;
		}
		for(int i=0; i<remain; i++){
			cluster.add(dataQuasis.get(pos));
			pos++;
		}
		cluster.calculateCentroid();
		pos -= remain;
		for(int i=0; i<remain; i++){
			for(int j=0; j<RecordQ.numAttr; j++){
				dataQuasis.get(pos).attrValues[j] = cluster.getCentroid().attrValues[j];
			}
			pos++;
		}
		
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Anonymizing done...");
		
                //AKKA fix: log
		//System.out.print("Rearranging...");
                LOGGER.trace("Rearranging...");
		Collections.sort(dataQuasis, new ComparatorID());
		for(int i=0; i<dataQuasis.size(); i++){	//anonymiza datos originales
			recordQ = dataQuasis.get(i);
			record = dataOri.get(i).clone();
			dataAnom.add(recordQ.toRecord(record));
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Rearranging done...");
		
                //AKKA fix: log
                LOGGER.debug("Anonymizing done (kAnonymity k = {})", k);

                return dataAnom;
	}
	
	/**
	 * This function applies k-anonymization + t-closeness to a dataset
	 * 
	 * @param dataOri, the dataset
	 * @param k, the desired k level
	 * @param t, the desired t closeness
	 * @return the anonymized version of the dataset that fullfils k-anonymity and t-closeness
	 */
	
	public static String[][] kAnonymize_tCloseness(String[][] dataOri, int k, double t){
		ArrayList<Record>data;
		ArrayList<Record>dataAnom;
		String[][] dataAnomStr;
	
		data = createRecords(dataOri);
		dataAnom = kAnonymize_tCloseness(data, k, t);
		dataAnomStr = createMatrixStringFromRecords(dataAnom);
		
		return dataAnomStr;
	}
	
	public static ArrayList<Record> kAnonymize_tCloseness(ArrayList<Record>dataOri, int k, double t){
		ArrayList<RecordQ>dataQuasis = new ArrayList<>();
		ArrayList<Record>dataAnom = new ArrayList<>();
		ArrayList<Cluster>clustersK = new ArrayList<Cluster>();
		ArrayList<Cluster>clusters = new ArrayList<Cluster>();
		RecordQ r;
		int n;
		int remain, numAttrQuasi, attrSensitive;
		int numItem, index, numClustersK, remainder;
		double kPrime;
		Cluster clusterTemp;
		RecordQ recordQ;
		String attrType;
		
                //AKKA fix: log
		//System.out.println("Anonymizing kAnonymity / tCloseness k = " + k + " / t = " + t);
                LOGGER.trace("Anonymizing kAnonymity / tCloseness k = {} / t = {}...", k, t);

		RecordQ.numAttr = Record.numQuasi + 1;
		RecordQ.listAttrTypes = new ArrayList<String>();
		RecordQ.listDataTypes = new ArrayList<String>();
		for(int i=0; i<Record.numAttr; i++){
			attrType = Record.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.quasiIdentifier)){
				RecordQ.listAttrTypes.add(Record.listAttrTypes.get(i));
				RecordQ.listDataTypes.add(Record.listDataTypes.get(i));
			}
		}
		for(int i=0; i<Record.numAttr; i++){
			attrType = Record.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.confidential)){
				RecordQ.listAttrTypes.add(Record.listAttrTypes.get(i));
				RecordQ.listDataTypes.add(Record.listDataTypes.get(i));
			}
		}
		for(Record reg:dataOri){	//crea records con solo los quasi + 1 sensible
			dataQuasis.add(reg.toRecordQConfidential());
		}
		
		Distances.calculateTypicalDeviationsNumericWithConfidential(dataQuasis);
                //AKKA fix: log
                //System.out.print("Sorting by confidential attribute...");
                LOGGER.trace("Sorting by confidential attribute...");
		attrSensitive = dataQuasis.get(0).attrValues.length-1;
		Functions.sortBySensitive(dataQuasis, attrSensitive);
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Sorting by confidential attribute done");
		
		n = dataQuasis.size();
		kPrime = n/(2*(n-1)*t+1);
		if(k > kPrime){
			numClustersK = k;
		}
		else{
			numClustersK = ((int)kPrime)+1;
		}
		numItem = dataQuasis.size() / numClustersK;
		remainder = dataQuasis.size() % numClustersK;
		
		if(remainder >= numItem){
			numClustersK = numClustersK+(remainder/numItem);
		}
		
                //AKKA fix: log
                //System.out.print("Creating k subsets(" + numClustersK + ")...");
                LOGGER.trace("Creating k subsets({})...", numClustersK);
		index = 0;
		for(int i=0; i<numClustersK; i++){
			clusterTemp = new Cluster();
			for(int j=0; j<numItem; j++){
				r = dataQuasis.get(index);
				clusterTemp.add(r);
				index++;
			}
			clustersK.add(clusterTemp);
		}
		
		if(index < dataQuasis.size()){	//remain records in a cluster
			clusterTemp = new Cluster();
			for(int i=index; i<dataQuasis.size(); i++){
				r = dataQuasis.get(i);
				clusterTemp.add(r);
			}
			clustersK.add(clusterTemp);
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Creating k subsets({}) done", numClustersK);
		
                //AKKA fix: log
                //System.out.print("Sorting by quasi-identifier attributes each subset...");
                LOGGER.trace("Sorting by quasi-identifier attributes each subset...");
		for(Cluster cluster:clustersK){
			Functions.sortByQuasi(cluster.getElements());
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Sorting by quasi-identifier attributes each subset done");
		
                //AKKA fix: log
                //System.out.print("Creating clusters...");
                LOGGER.trace("Creating clusters...");
		remain = dataQuasis.size();
		dataQuasis.clear();
		index = 0;
		while(remain > 0){
			clusterTemp = new Cluster();
			for(Cluster cluster:clustersK){
				if(cluster.getElements().size() > index){
					clusterTemp.add(cluster.getElements().get(index));	//the next record is added
					remain--;
				}
			}
			index++;
			clusters.add(clusterTemp);	
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Creating clusters done");
		
                //AKKA fix: log
                //System.out.print("Anonymizing...");
                LOGGER.trace("Anonymizing...");
		numAttrQuasi = clusters.get(0).getElements().get(0).attrValues.length - 1;
		for(Cluster cluster:clusters){
			cluster.calculateCentroid();
			for(RecordQ reg:cluster.getElements()){
				for(int j=0; j<numAttrQuasi; j++){
					reg.attrValues[j] = cluster.getCentroid().attrValues[j];
				}
				dataQuasis.add(reg);
			}
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Anonymizing done");
		
                //AKKA fix: log
                //System.out.print("ReArranging...");
                LOGGER.trace("ReArranging...");
		Collections.sort(dataQuasis, new ComparatorID());
		for(int i=0; i<dataQuasis.size(); i++){
			recordQ = dataQuasis.get(i);
			dataAnom.add(recordQ.toRecord(dataOri.get(i)));
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("ReArranging done");
		
                //AKKA fix: log
                LOGGER.debug("Anonymizing done (kAnonymity / tCloseness k = {} / t = {})", k, t);

                return dataAnom;
	}
	
	/**
	 * This function applies coarsening shift to a dataset 
	 * 
	 * @param dataOri, the dataset
	 * @param radius, the desired level of privacy (radius of circle)
	 * @return the anonymized version of the dataset that fullfils k-anonymity
	 */
	
        //AKKA fix: radius value depends on SRID. it could be a real
        //public static String[][] coarseningShift(String[][] dataOri, int radius){
	public static String[][] coarseningShift(String[][] dataOri, double radius){
		ArrayList<Record>data;
		ArrayList<Record>dataAnom;
		String[][] dataAnomStr;
	
		data = createRecords(dataOri);
		dataAnom = coarseningShift(data, radius);
		dataAnomStr = createMatrixStringFromRecords(dataAnom);
		
		return dataAnomStr;
	}
	
        //AKKA fix: radius value depends on SRID. it could be a real
	//public static ArrayList<Record> coarseningShift(ArrayList<Record>dataOri, int radius){
        public static ArrayList<Record> coarseningShift(ArrayList<Record>dataOri, double radius){
		ArrayList<Record>dataAnom = new ArrayList<Record>();
		ArrayList<String>geometricObjects = new ArrayList<String>();
		ArrayList<String>geometricObjectsAnom = new ArrayList<String>();
		int posGeom;
		String attrType, dataType, geomStr;
		Geometry geom; //Objecte geometric basic
		//AKKA 1st review: use WKT
		//WKBReader reader = new WKBReader(); //Parseja objectes en format WKB (Well Known Binary)
		WKTReader reader = new WKTReader();
		WKBWriter writer = new WKBWriter(2,true); //Converteix objectes de GeoTools
		Geometry cir;
		double x, y;
		Circle circle;
		Record record, recordAnom;
		
                //AKKA fix: log
                //System.out.println("Coarsening radius = " + radius + "...");
                LOGGER.trace("Coarsening radius = {}...", radius);
		posGeom = 0;
		for(int i=0; i<Record.numAttr; i++){	//posicio del geometric_object
			attrType = Record.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.identifier)){
				dataType = Record.listDataTypes.get(i);
				if(dataType.equalsIgnoreCase(Constants.geometricObject)){
					posGeom = i;
					break;
				}
			}
		}
		
		for(Record reg:dataOri){
			geomStr = reg.attrValues[posGeom];
			geometricObjects.add(geomStr);
		}
		for(String s:geometricObjects){	//extreu i converteix coordenada a cercle
			try {
		                //AKKA 1st review: use WKT
				//geom = reader.read(WKBReader.hexToBytes(s));
			        int srid = 0;
			        if (s.startsWith("SRID")) {
			            int begin = s.indexOf('=') + 1;
                                    int end = s.indexOf(';', begin);
			            srid = Integer.parseInt(s.substring(begin, end));
			            s = s.substring(end + 1);
			        }
                                geom = reader.read(s);
                                geom.setSRID(srid); 
				x = geom.getCoordinate().x;
				y = geom.getCoordinate().y;
				circle = shift(x, y, radius);
	                        //AKKA fix: radius unit and value depends on SRID. Don't convert it
				cir = create3DCircle(circle.centre.latitude, circle.centre.longitude, radius);
				//AKKA fix: SRID of Polygon must be the same than SRID of original Point
                                ////AKKA fix: inverse latitude and longitude
                                ////cir.setSRID(calculateSrid(circle.centre.latitude, circle.centre.longitude, 4326));
				cir.setSRID(geom.getSRID());
				geometricObjectsAnom.add(WKBWriter.toHex(writer.write(cir)));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		for(int i=0; i<dataOri.size(); i++){
			record = dataOri.get(i);
			recordAnom = record.clone();
			recordAnom.attrValues[posGeom] = geometricObjectsAnom.get(i);
			dataAnom.add(recordAnom);
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Coarsening done (radius = {})", radius);
		
		return dataAnom;
	}
	
	/**
	 * This function applies coarsening microaggregation to a dataset 
	 * 
	 * @param dataOri, the dataset
	 * @param k, the desired level of privacy
	 * @return the anonymized version of the dataset that fullfils k-anonymity
	 */
	
	public static String[][] coarseningMicroaggregation(String[][] dataOri, int k){
		ArrayList<Record>data;
		ArrayList<Record>dataAnom;
		String[][] dataAnomStr;
	
		data = createRecords(dataOri);
		dataAnom = coarseningMicroaggregation(data, k);
		dataAnomStr = createMatrixStringFromRecords(dataAnom);
		
		return dataAnomStr;
	}
	
	public static ArrayList<Record> coarseningMicroaggregation(ArrayList<Record>dataOri, int k){
		ArrayList<Record>dataAnom = new ArrayList<Record>();
		ArrayList<String>geometricObjectsAnom = new ArrayList<String>();
		ArrayList<CoordinateS>pointsAnom = new ArrayList<CoordinateS>();
		ArrayList<Circle>circles = new ArrayList<Circle>();
		ArrayList<ClusterPoints>clusters = new ArrayList<ClusterPoints>();
		CoordinateS centroid, farthest, closest;
		ClusterPoints cluster;
		double distance;
		Circle circle;
		int posGeom;
		String attrType, dataType, geomStr;
		Geometry geom; //Objecte geometric basic
                //AKKA 1st review: use WKT
		//WKBReader reader = new WKBReader(); //Parseja objectes en format WKB (Well Known Binary)
                WKTReader reader = new WKTReader();
		WKBWriter writer = new WKBWriter(2,true); //Converteix objectes de GeoTools
		Geometry cir;
		double x, y;
		Record record, recordAnom;
		
                //AKKA fix: log
                //System.out.println("Coarsening microaggregation k = " + k + "...");
                LOGGER.trace("Coarsening microaggregation k = {}...", k);
		
		posGeom = 0;
		for(int i=0; i<Record.numAttr; i++){	//posicio del geometric_object
			attrType = Record.listAttrTypes.get(i);
			if(attrType.equalsIgnoreCase(Constants.identifier)){
				dataType = Record.listDataTypes.get(i);
				if(dataType.equalsIgnoreCase(Constants.geometricObject)){
					posGeom = i;
					break;
				}
			}
		}
		for(Record reg:dataOri){
			try {
				geomStr = reg.attrValues[posGeom];
                                //AKKA 1st review: use WKT
                                //geom = reader.read(WKBReader.hexToBytes(geomStr));
                                if (geomStr.startsWith("SRID")) {
                                    geomStr = geomStr.substring(geomStr.indexOf(';') + 1);
                                }
                                int srid = 0;
                                if (geomStr.startsWith("SRID")) {
                                    int begin = geomStr.indexOf('=') + 1;
                                    int end = geomStr.indexOf(';', begin);
                                    srid = Integer.parseInt(geomStr.substring(begin, end));
                                    geomStr = geomStr.substring(end + 1);
                                }
                                geom = reader.read(geomStr);
                                geom.setSRID(srid); 
				x = geom.getCoordinate().x;
				y = geom.getCoordinate().y;
                                //AKKA fix: inverse latitude and longitude
				//Moreover, save SRID of original Point
				//pointsAnom.add(new CoordinateS(x, y, reg.id));
				pointsAnom.add(new CoordinateS(y, x, reg.id, geom.getSRID()));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		while(pointsAnom.size() >= k){
			centroid = calculateCentroid(pointsAnom);
			farthest = calculateFarthestPoint(centroid, pointsAnom);
			cluster = new ClusterPoints();
			cluster.add(farthest);
			pointsAnom.remove(farthest);
			centroid = cluster.calculateCentroid();
			while(cluster.getNumPoints() < k){
				closest = calculateClosestPoint(centroid, pointsAnom);
				cluster.add(closest);
				pointsAnom.remove(closest);
				centroid = cluster.calculateCentroid();
			}
			clusters.add(cluster);
		}
		for(CoordinateS p:pointsAnom){	//remaining points to its closest cluster
			cluster = calculateClosestCluster(p, clusters);
			cluster.add(p);
			cluster.calculateCentroid();
		}
		for(ClusterPoints c:clusters){	//calculates radius circle (max distance to centroid) 
			centroid = c.getCentroid();
			farthest = calculateFarthestPoint(centroid, c.getPoints());
			//AKKA fix: centroid.distanceSq returns square of distance, so take the square root
                        distance = Math.sqrt(centroid.distanceSq(farthest));
			for(CoordinateS coo:c.getPoints()){	//each point of cluster has the same coordinates
				circle = new Circle(centroid, distance);
				circle.centre.id = coo.id;
                                //AKKA fix: save SRID of original Point
				circle.centre.srid = coo.srid;
				circles.add(circle);
			}
		}
		Collections.sort(circles, new ComparatorIdCircles());
		
		for(Circle c:circles){
                        //AKKA fix: radius unit depends on SRID. Don't convert it
                        //cir = create3DCircle(c.centre.latitude, c.centre.longitude, (c.radius/1852));
                        cir = create3DCircle(c.centre.latitude, c.centre.longitude, c.radius);
                        //AKKA fix: SRID of Polygon must be the same than SRID of original Point
                        ////AKKA fix: inverse latitude and longitude
                        ////cir.setSRID(calculateSrid(c.centre.latitude, c.centre.longitude, 4326));
                        cir.setSRID(c.centre.srid);
			geometricObjectsAnom.add(WKBWriter.toHex(writer.write(cir)));
		}
		
		for(int i=0; i<dataOri.size(); i++){
			record = dataOri.get(i);
			recordAnom = record.clone();
			recordAnom.attrValues[posGeom] = geometricObjectsAnom.get(i);
			dataAnom.add(recordAnom);
		}
                //AKKA fix: log
                //System.out.println("done");
                LOGGER.debug("Coarsening microaggregation done (k = {})", k);
		
		return dataAnom;
	}
	
	private static ClusterPoints calculateClosestCluster(CoordinateS point, ArrayList<ClusterPoints>clusters){
		ClusterPoints closest;
		double minDistance, distance;
		
		closest = null;
		minDistance = Double.MAX_VALUE;
		for(ClusterPoints c:clusters){
			distance = c.getCentroid().distanceSq(point);
			if(distance < minDistance){
				closest = c;
				minDistance = distance;
			}
		}
		
		return closest;
	}
	
	private static CoordinateS calculateClosestPoint(CoordinateS point, ArrayList<CoordinateS>points){
		CoordinateS closest;
		double minDistance, distance;
		
		closest = null;
		minDistance = Double.MAX_VALUE;
		for(CoordinateS p:points){
			distance = p.distanceSq(point);
			if(distance < minDistance){
				closest = p;
				minDistance = distance;
			}
		}
		
		return closest;
	}
	
	private static CoordinateS calculateFarthestPoint(CoordinateS point, ArrayList<CoordinateS>points){
		CoordinateS farthest;
		double maxDistance, distance;
		
		farthest = null;
		maxDistance = 0;
		for(CoordinateS p:points){
			distance = p.distanceSq(point);
			if(distance > maxDistance){
				farthest = p;
				maxDistance = distance;
			}
		}
		
		return farthest;
	}
	
	private static CoordinateS calculateCentroid(ArrayList<CoordinateS>points){
		CoordinateS centroid;
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
	
	public static int calculateSrid(double x, double y, int srid) {
		int pref;
		int zone;
		
		if(y > 0) pref = 32600;
		else pref = 32700;
		
		zone = ((int)((x+180)/6))+1; //Casting a double to int behaves as we want (Drops the decimals)
		
		return zone+pref;
	}
	
        //AKKA fix: inverse latitude and longitude
	//private static Geometry create3DCircle(double lng, double lat, double radiusNm) { 
	private static Geometry create3DCircle(double lat, double lng, double radius) {
		PrecisionModel pmodel = new PrecisionModel(); //No podem especificar un SRID al GeometryFactory sense passarli un PrecisionModel
		//AKKA fix: don't need SRID here 
		//GeometryFactory builder = new GeometryFactory(pmodel, 4326); //GeometryFactory crea objectes geometrics de gis
                GeometryFactory builder = new GeometryFactory(pmodel); //GeometryFactory crea objectes geometrics de gis
//AKKA fix: use GeometryBuilder.circle() to create the polygon
//        GeodeticCalculator calc = new GeodeticCalculator(DefaultEllipsoid.WGS84); 
//        calc.setStartingGeographicPoint(lng, lat); 
//        final int SIDES = 32 + 16 * ((int)Math.ceil(radiusNm / 40) / 5);       // Fairly random. 
//
//        double distance = radiusNm * 1852      /*1855.3248*/;              // Convert to metres.	
//        double baseAzimuth = 360.0 / SIDES; 
//        Coordinate coords[] = new Coordinate[SIDES+1]; 
//        for( int i = 0; i < SIDES; i++){ 
//                double azimuth = 180 - (i * baseAzimuth); 
//                calc.setDirection(azimuth, distance); 
//                Point2D point = calc.getDestinationGeographicPoint(); 
//                coords[i] = new Coordinate(point.getX(), point.getY()); 
//        } 
//        coords[SIDES] = coords[0]; 
//
//        LinearRing ring = builder.createLinearRing( coords ); 
//        Polygon polygon = builder.createPolygon( ring, null ); 
        GeometryBuilder gb = new GeometryBuilder(builder);
        final int SIDES = 32 + 16 * new Random().nextInt(4);       // Random.
        Polygon polygon = gb.circle(lng, lat, radius, SIDES);
        return polygon; 
	}
	
        //AKKA fix: radius unit depends on SRID. Don't convert it
//	public static Circle shift(double x1, double y1, int metersPrivacy) {
//		return shift(x1, y1, (double)metersPrivacy/111220); //111220 metres per grau
//	}
	
	public static Circle shift(double x1, double y1, double privacy) {
		double theta = ThreadLocalRandom.current().nextDouble((360-0)+1);
		double x2 = x1 + (Math.cos(theta)*privacy);
		double y2 = y1 + (Math.sin(theta)*privacy);
		
		//System.out.println(theta);
		//System.out.println("cos(theta) = "+Math.cos(Math.toRadians(theta)));
		//System.out.println("sin(theta) = "+Math.sin(Math.toRadians(theta)) );
		
		//System.out.println(eu.clarussecure.dataoperations.anonymization.Distances.distanciaHaversine(x1, y1, x2, y2));
		
		//System.out.println("max distance: "+x2+" "+y2);
		
		double x = x1 + ((x2-x1) * ThreadLocalRandom.current().nextDouble());
		double y = y1 + ((y2-y1) * ThreadLocalRandom.current().nextDouble());
		
		//System.out.println(y+", "+x); //prints in google maps style (latitude and longitude)
		
		return new Circle(x,y,privacy);
	}
	
	private static void sortByQuasi(ArrayList<RecordQ> data){
		
		ComparatorQuasi.setAttributeSortCriteria(data.get(0));
		Collections.sort(data, new ComparatorQuasi());
	}
	
	private static void sortBySensitive(ArrayList<RecordQ>data, int attr){
		ComparatorSensitive.setAttributeSortCriteria(attr);
		Collections.sort(data, new ComparatorSensitive());
	}
	
	public static void readPropertiesFromFile(String fileProperties){
		Document document;
		
		document = readDocumentFromFile(fileProperties);
		readProperties(document);
	}
	
	public static void readProperties(String xml){
		Document document;
		
		document = readDocument(xml);
		readProperties(document);
	}
	
	public static void readProperties(byte[] xml){
		Document document;
		
		document = readDocument(xml);
		readProperties(document);
	}
	
	private static Document readDocumentFromFile(String fileProperties){
		Document document = null;
		
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			document = db.parse(new File(fileProperties));
			document.getDocumentElement().normalize();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return document;
	}
	
	private static Document readDocument(String xml){
		Document document = null;
		
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			InputSource is = new InputSource(new StringReader(xml));
			document = db.parse(is);
			document.getDocumentElement().normalize();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return document;
	}
	
	public static Document readDocument(byte[] xml){
		Document document = null;
		
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			InputSource is = new InputSource(new StringReader(new String(xml)));
			document = db.parse(is);
			document.getDocumentElement().normalize();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return document;
	}
	
	public static void readProperties(Document document){
		int numQuasis;
		
		Record.header = hasHeader(document);
		Record.attributeSeparator = getAttributeSeparator(document);
		Record.recordSeparator = getRecordSeparator(document);
		
		Record.attrTypes = getAttributeTypes(document);
		for(String s:Record.attrTypes.values()){
			if(s.equalsIgnoreCase(Constants.kAnonymity)){
				Record.k = Integer.parseInt(getK(document));
			}
			if(s.equalsIgnoreCase(Constants.tCloseness)){
				Record.t = Double.parseDouble(getT(document));
			}
			if(s.equalsIgnoreCase(Constants.splitting)){
				Record.clouds = Integer.parseInt(getClouds(document));
			}
			if(s.equalsIgnoreCase(Constants.encryption)){
				Record.idKey = getIdKey(document);
			}
			if(s.equalsIgnoreCase(Constants.coarsening)){
				Record.coarsening_type = getCoarseningType(document);
				if(Record.coarsening_type.equalsIgnoreCase(Constants.shift)){
				        //AKKA fix: radius value depends on SRID. it could be a real
					//Record.radius = Integer.parseInt(getRadius(document));
                                        Record.radius = Double.parseDouble(getRadius(document));
				}
				if(Record.coarsening_type.equalsIgnoreCase(Constants.microaggregation)){
					Record.k=Integer.parseInt(getCoarseningK(document));
				}
			}
		}
		Record.listNames = getAtributeNames(document);
		Record.listAttrTypes = getAtributeTypes(document);
		numQuasis = 0;
		for(String s:Record.listAttrTypes){
			if(s.equals(Constants.quasiIdentifier)){
				numQuasis++;
			}
		}
		Record.numQuasi = numQuasis;
		if(Record.numQuasi == 0){
			Record.attrTypes.put(Constants.quasiIdentifier, "null");
		}
		Record.listDataTypes = getAttributeDataTypes(document);
		Record.numAttr = Record.listAttrTypes.size();
	}
	
	private static boolean hasHeader(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String header;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(1);
		atributos = nodo.getAttributes();
		nodo = atributos.getNamedItem(Constants.header);
		//AKKA fix: header is optional
		if (nodo == null) {
                    return false;
		}
		header = nodo.getNodeValue();
		if(header.equalsIgnoreCase(Constants.no)){
			return false;
		}
		return true;
	}
	
	private static String getAttributeSeparator(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String separator;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(1);
		atributos = nodo.getAttributes();
		nodo = atributos.getNamedItem(Constants.attributeSeparator);
                //AKKA fix: attributeSeparator is optional
                if (nodo == null) {
                    return null;
                }
		separator = nodo.getNodeValue();
		
		return separator;
	}
	
	private static String getRecordSeparator(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String separator;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(1);
		atributos = nodo.getAttributes();
		nodo = atributos.getNamedItem(Constants.recordSeparator);
                //AKKA fix: recordSeparator is optional
                if (nodo == null) {
                    return null;
                }
		separator = nodo.getNodeValue();
		
		return separator;
	}
	
	private static HashMap<String,String> getAttributeTypes(Document document){
		HashMap<String,String>attrTypes = new HashMap<String,String>();
		Node raiz, nodo;
		NamedNodeMap atributos;
		String type, protection;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.type);
			type = nodo.getNodeValue();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			attrTypes.put(type, protection);
		}
		
		return attrTypes;
	}
	
	private static String getK(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String protection;
		String k = null;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			if(protection.equalsIgnoreCase(Constants.kAnonymity)){
				nodo = atributos.getNamedItem(Constants.k);
				k = nodo.getNodeValue(); 
			}
		}
		
		return k;
	}
	
	private static String getT(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String protection;
		String k = null;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			if(protection.equalsIgnoreCase(Constants.tCloseness)){
				nodo = atributos.getNamedItem(Constants.t);
				k = nodo.getNodeValue(); 
			}
		}
		
		return k;
	}
	
	private static String getClouds(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String protection;
		String clouds = null;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			if(protection.equalsIgnoreCase(Constants.splitting)){
				nodo = atributos.getNamedItem(Constants.clouds);
				clouds = nodo.getNodeValue(); 
			}
		}
		
		return clouds;
	}
	
	private static String getIdKey(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String protection;
		String idKey = null;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			if(protection.equalsIgnoreCase(Constants.encryption)){
				nodo = atributos.getNamedItem(Constants.id_key);
				idKey = nodo.getNodeValue(); 
			}
		}
		
		return idKey;
	}
	
	private static String getRadius(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String protection;
		String radius = null;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			if(protection.equalsIgnoreCase(Constants.coarsening)){
				nodo = atributos.getNamedItem(Constants.radius);
				radius = nodo.getNodeValue(); 
			}
		}
		
		return radius;
	}
	
	private static String getCoarseningType(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String protection;
		String type = null;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			if(protection.equalsIgnoreCase(Constants.coarsening)){
				nodo = atributos.getNamedItem(Constants.coarseningType);
				type = nodo.getNodeValue(); 
			}
		}
		
		return type;
	}
	
	private static String getCoarseningK(Document document){
		Node raiz, nodo;
		NamedNodeMap atributos;
		String protection;
		String k = null;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(3);
		nodeList = nodo.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.protection);
			protection = nodo.getNodeValue();
			if(protection.equalsIgnoreCase(Constants.coarsening)){
				nodo = atributos.getNamedItem(Constants.k);
				k = nodo.getNodeValue(); 
			}
		}
		
		return k;
	}
	
	private static ArrayList<String> getAtributeNames(Document document){
		ArrayList<String>names = new ArrayList<String>();
		Node raiz, nodo;
		NamedNodeMap atributos;
		String name;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.name);
			name = nodo.getNodeValue();
			names.add(name);
		}
		
		return names;
	}
	
	private static ArrayList<String> getAtributeTypes(Document document){
		ArrayList<String>attrTypes = new ArrayList<String>();
		Node raiz, nodo;
		NamedNodeMap atributos;
		String attrType;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.attributeType);
			attrType = nodo.getNodeValue();
			attrTypes.add(attrType);
		}
		
		return attrTypes;
	}
	
	private static ArrayList<String> getAttributeDataTypes(Document document){
		ArrayList<String>attrTypes = new ArrayList<String>();
		Node raiz, nodo;
		NamedNodeMap atributos;
		String attrType;
		NodeList nodeList;
		
		raiz = document.getFirstChild();
		nodeList = raiz.getChildNodes();
		nodo = nodeList.item(1);
		nodeList = nodo.getChildNodes();
		for(int i=1; i<nodeList.getLength(); i+=2){
			nodo = nodeList.item(i);
			atributos = nodo.getAttributes();
			nodo = atributos.getNamedItem(Constants.dataType);
			if(nodo == null){
				attrTypes.add("");
			}
			else{
				attrType = nodo.getNodeValue();
				attrTypes.add(attrType);
			}
		}
		
		return attrTypes;
	}
	
	public static ArrayList<Record> createRecords(String data){
                //AKKA fix: log
                LOGGER.trace("Loading records...");

                ArrayList<Record> records = new ArrayList<Record>();
		String recordsStr[];
		String strTemp[];
		Record record;
		int id;
		
		recordsStr = data.split(Record.recordSeparator);
		id = 0;
		for(int i=0; i<recordsStr.length; i++){
			strTemp = recordsStr[i].split(Record.attributeSeparator);
			record = new Record(id);
			id++;
			for(int j=0; j<Record.numAttr; j++){
				record.attrValues[j] = strTemp[j];
			}
			records.add(record);
		}
		
		//AKKA fix: log
                //System.out.println("Records loaded: " + records.size());
		LOGGER.debug("Records loaded: {}", records.size());
		return records;
	}
	
	public static ArrayList<Record> createRecords(String[][] data){
                //AKKA fix: log
                LOGGER.trace("Loading records...");
		ArrayList<Record> records = new ArrayList<Record>();
		Record record = null;
		int id;
		
		id = 0;
		for(int i=0; i<data.length; i++){
			record = new Record(id);
			id++;
			for(int j=0; j<data[i].length; j++){
				record.attrValues[j] = data[i][j];
			}
			records.add(record);
		}
		
                //AKKA fix: log
                //System.out.println("Records loaded: " + records.size());
                LOGGER.debug("Records loaded: {}", records.size());
		return records;
	}
	
	public static String[][] createMatrixStringFromRecords(ArrayList<Record>records){
                //AKKA fix: log
                LOGGER.trace("Converting {} records to String matrix", records.size());
		String data[][];
		Record record;
		
		data = new String[records.size()][];
		for(int i=0; i<records.size(); i++){
			record = records.get(i);
			data[i] = record.toVectorString();
		}
		
                //AKKA fix: log
                //System.out.println(data.length + " records converted to String matrix");
                LOGGER.debug("{} records converted to String matrix", data.length);
		return data;
	}
	
	public static ArrayList<Record> readFile(String fileStr, String fileProperties){
                //AKKA fix: log
                LOGGER.trace("Loading records...");
		ArrayList<Record> records = new ArrayList<Record>();
		FileReader2 file;
		String linea;
		String strTemp[];
		Record record;
		int id;
		
		readPropertiesFromFile(fileProperties);
		file = new FileReader2(fileStr);
		if(Record.header){
			linea=file.readLine();
		}
		id = 0;
		while((linea=file.readLine())!=null){
			strTemp = linea.split(Record.attributeSeparator);
			record = new Record(id);
			id++;
			for(int i=0; i<Record.numAttr; i++){
				record.attrValues[i] = strTemp[i];
			}
			records.add(record);
		}
		file.closeFile();
                //AKKA fix: log
                //System.out.println("Records loaded: " + records.size());
                LOGGER.debug("Records loaded: {}", records.size());
		return records;
	}

    @Deprecated
	public static void writeFile(ArrayList<ArrayList<Record>>data){
		File file;
		FileWriter fw;
		BufferedWriter bw;
		String fileName;
		int cont;
		
		for(int i=0; i<data.size(); i++){
			cont = 0;
			if(Record.header){
				addCabecera(data.get(i));
				cont = -1;
			}
			fileName = "data_clarus_anom_" + (i+1) + ".txt";
			file = new File(fileName);
			try {
				fw = new FileWriter (file);
				bw = new BufferedWriter(fw);
				for(Record r:data.get(i)){
					bw.write(r.toString());
					bw.newLine();
					cont++;
				}
				bw.close();
				fw.close();

				System.out.println("Records saved: " + cont);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private static void addCabecera(ArrayList<Record>lista){
		Record record;
		
		record = new Record(0);
		for(int i=0; i<Record.listNames.size(); i++){
			record.attrValues[i] = Record.listNames.get(i);
		}
		lista.add(0, record);
	}

}
