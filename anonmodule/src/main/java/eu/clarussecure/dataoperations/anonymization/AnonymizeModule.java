package eu.clarussecure.dataoperations.anonymization;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.Operation;
import eu.clarussecure.dataoperations.Promise;

public class AnonymizeModule implements DataOperation {

	public AnonymizeModule(Document document){
		Functions.readProperties(document);
	}

	@Override
	public String[][] head(String[] attributeNames) {
                //TODO: not yet implemented
                return null;
	}

	@Override
	public Promise get(String[] strings, String[] strings1, Operation operation, boolean dispatch) {
		//TODO: not yet implemented
		return null;
	}

	@Override
	public String[][] get(Promise promise, String[][] strings) {
		//TODO: not yet implemented
		return new String[0][];
	}

	@Override
	public String[][][] post(String[] attributes, String[][] content) {
		String[][] plainDataAnom;

		plainDataAnom = Functions.anonymize(attributes, content);

		return new String[][][] { plainDataAnom };
	}

	@Override
	public String[][][] put(String[] strings, String[] strings1, String[][] strings2) {
            return new String[][][] { strings2 };
	}

	@Override
	public String[][] delete(String[] strings, String[] strings1) {
            return new String[][] { strings };
	}

}
