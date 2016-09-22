package eu.clarussecure.dataoperations;

public interface Promise {
    public int getId();
    public String[] getAttributeNames();
    public String getOperation();
    public String[][] getCall();
}
