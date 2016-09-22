package eu.clarussecure.dataoperations.anonymization;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

public class DatasetParser {

    private final File f;
    private final String fieldSeparator;

    public DatasetParser(File f, String fieldSeparator) {
        this.f = f;
        this.fieldSeparator = fieldSeparator;
    }

    public String[] parseHeaders() throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(this.f));
        String line = in.readLine();
        in.close();
        return line.split(this.fieldSeparator);
    }

    public String[][] parseDataset() throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(this.f));
        LinkedList<String> lines = new LinkedList<String>();
        String line = "";
        while ((line = in.readLine()) != null) {
            lines.add(line);
        }
        in.close();
        line = lines.pollFirst();
        int recordSize = line.split(this.fieldSeparator).length;
        String[][] content = new String[lines.size()][recordSize];
        for (int i = 0; i < lines.size(); i++) {
            content[i] = lines.get(i).split(this.fieldSeparator);
        }
        return content;
    }

    public String[][] getSingleRecord() throws IOException {
        String[][] dataset = parseDataset();
        Random r = new Random();
        String[][] record = new String[1][dataset[0].length];
        record[0] = dataset[r.nextInt() % dataset.length];
        return record;
    }
}
