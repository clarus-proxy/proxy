package eu.clarussecure.dataoperations.anonymization;

import java.io.File;
import java.io.IOException;

import eu.clarussecure.dataoperations.DataOperation;
import org.w3c.dom.Document;

public class Principal {

	public static void main(String[] args) throws IOException {
		String[] attributes;
		String[][] dataOri, dataAnom;
		byte[] xmlProperties;
		Document document;
		DatasetParser datasetParser;
		File file;
		
		xmlProperties = loadXmlFile("properties.xml");
		document = Functions.readDocument(xmlProperties);
		DataOperation interFace = new AnonymizeModule(document);
		
		file = new File("data_clarus2.txt");
		datasetParser = new DatasetParser(file , ",");
		
		attributes = datasetParser.parseHeaders();
		dataOri = datasetParser.parseDataset();
		dataAnom = interFace.post(attributes, dataOri);
		
//		xmlProperties = loadXmlFile("properties2.xml");
//		document = eu.clarussecure.dataoperations.anonymization.Functions.readDocument(xmlProperties);
//		ClarusInterface interFace = new eu.clarussecure.dataoperations.anonymization.AnonymizeModule(document);
//		
//		file = new File("data_clarus2.txt");
//		datasetParser = new eu.clarussecure.dataoperations.anonymization.DatasetParser(file , ",");
//		
//		attributes = datasetParser.parseHeaders();
//		dataOri = datasetParser.parseDataset();
//		dataAnom = interFace.post(attributes, dataOri);
		
//		xmlProperties = loadXmlFile("properties4.xml");
//		document = eu.clarussecure.dataoperations.anonymization.Functions.readDocument(xmlProperties);
//		ClarusInterface interFace = new eu.clarussecure.dataoperations.anonymization.AnonymizeModule(document);
//		
//		file = new File("boreholes2.txt");
//		datasetParser = new eu.clarussecure.dataoperations.anonymization.DatasetParser(file , ";");
//		
//		attributes = datasetParser.parseHeaders();
//		dataOri = datasetParser.parseDataset();
//		dataAnom = interFace.post(attributes, dataOri);
		
//		xmlProperties = loadXmlFile("properties5.xml");
//		document = eu.clarussecure.dataoperations.anonymization.Functions.readDocument(xmlProperties);
//		ClarusInterface interFace = new eu.clarussecure.dataoperations.anonymization.AnonymizeModule(document);
//		
//		file = new File("boreholes2.txt");
//		datasetParser = new eu.clarussecure.dataoperations.anonymization.DatasetParser(file , ";");
//		
//		attributes = datasetParser.parseHeaders();
//		dataOri = datasetParser.parseDataset();
//		dataAnom = interFace.post(attributes, dataOri);
		
//		xmlProperties = loadXmlFile("properties4.xml");
//		document = eu.clarussecure.dataoperations.anonymization.Functions.readDocument(xmlProperties);
//		ClarusInterface interFace = new eu.clarussecure.dataoperations.anonymization.AnonymizeModule(document);
//		
//		file = new File("boreholes2.txt");
//		datasetParser = new eu.clarussecure.dataoperations.anonymization.DatasetParser(file , ";");
//		
//		attributes = datasetParser.parseHeaders();
//		dataOri = datasetParser.getSingleRecord();
//		dataAnom = interFace.post(attributes, dataOri);
	}
	
	public static byte[] loadXmlFile(String filePropertiesName){
		FileReader2 file;
		String linea;
		String xml;
		
		file = new FileReader2(filePropertiesName);
		xml = "";
		while((linea=file.readLine())!=null){
			xml += linea;
		}
		file.closeFile();
		Functions.readProperties(xml);
		System.out.println("Xml loaded");
		return xml.getBytes();
	}
}