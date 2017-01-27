package eu.clarussecure.proxy.spi.protection;

import eu.clarussecure.dataoperations.Promise;

public class DefaultPromise implements Promise {
    private int id;
    private String[] attributeNames;
    private String[] criteria;
    private String[][] protectedAttributeNames;
    private int[][][] attributeMapping;
    private String[][] protectedCriteria;
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

    public String[] getCriteria() {
        return criteria;
    }

    public void setCriteria(String[] criteria) {
        this.criteria = criteria;
    }

    public String[][] getProtectedAttributeNames() {
        return protectedAttributeNames;
    }

    public void setProtectedAttributeNames(String[][] protectedAttributeNames) {
        this.protectedAttributeNames = protectedAttributeNames;
    }

    public int[][][] getAttributeMapping() {
        return this.attributeMapping;
    }

    public void setAttributeMapping(int[][][] attributeMapping) {
        this.attributeMapping = attributeMapping;
    }

    public String[][] getProtectedCriteria() {
        return protectedCriteria;
    }

    public void setProtectedCriteria(String[][] protectedCriteria) {
        this.protectedCriteria = protectedCriteria;
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
