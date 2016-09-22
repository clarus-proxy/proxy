package eu.clarussecure.dataoperations.anonymization;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileReader2 {
	private BufferedReader br;
	
	public FileReader2(String file) {
		FileInputStream fis = null;
		try {
            // Open the file
            fis = new FileInputStream(file);            
            br = new BufferedReader(new InputStreamReader(fis));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
	}
	
	public String readLine(){
		String line;
		try {
			if((line = br.readLine()) != null) return line;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public int closeFile() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
}
