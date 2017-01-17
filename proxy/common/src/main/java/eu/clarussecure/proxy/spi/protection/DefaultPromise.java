package eu.clarussecure.proxy.spi.protection;

import eu.clarussecure.dataoperations.Promise;

public class DefaultPromise implements Promise {
    private int id;
    private String[] attributeNames;
    private String operation;
    private String[][] call;

    @Override
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String[] getAttributeNames() {
        return attributeNames;
    }

    public void setAttributeNames(String[] attributeNames) {
        this.attributeNames = attributeNames;
    }

    @Override
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    @Override
    public String[][] getCall() {
        return call;
    }

    public void setCall(String[][] call) {
        this.call = call;
    }

}
