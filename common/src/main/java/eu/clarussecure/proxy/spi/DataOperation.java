package eu.clarussecure.proxy.spi;

import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.dataoperations.DataOperationCommand;

public class DataOperation extends ModuleOperation {
    private int requestId;
    private Operation operation;
    private boolean usingHeadOperation;
    private List<CString> dataIds;
    private List<List<CString>> dataValues;
    private List<DataOperationCommand> promise;
    private boolean unprotectingDataEnabled = true;

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public boolean isUsingHeadOperation() {
        return usingHeadOperation;
    }

    public void setUsingHeadOperation(boolean usingHeadOperation) {
        this.usingHeadOperation = usingHeadOperation;
    }

    @Override
    public List<CString> getDataIds() {
        if (dataIds == null) {
            dataIds = new ArrayList<>();
        }
        return dataIds;
    }

    @Override
    public void setDataIds(List<CString> dataIds) {
        this.dataIds = dataIds;
    }

    @Override
    public void addDataId(CString dataId) {
        getDataIds().add(dataId);
    }

    @Override
    public void addDataIds(List<CString> dataIds) {
        dataIds.forEach(dataId -> addDataId(dataId));
    }

    @Override
    public void removeDataId(int index) {
        getDataIds().remove(index);
    }

    public List<List<CString>> getDataValues() {
        if (dataValues == null) {
            dataValues = new ArrayList<>();
        }
        return dataValues;
    }

    public void setDataValues(List<List<CString>> dataValues) {
        this.dataValues = dataValues;
    }

    public void addDataValues(List<List<CString>> dataValues) {
        dataValues.forEach(dataValue -> addDataValue(dataValue));
    }

    public void addDataValue(List<CString> dataValue) {
        getDataValues().add(dataValue);
    }

    public List<DataOperationCommand> getPromise() {
        return promise;
    }

    public void setPromise(List<DataOperationCommand> promise) {
        this.promise = promise;
    }

    public boolean isUnprotectingDataEnabled() {
        return unprotectingDataEnabled;
    }

    public void setUnprotectingDataEnabled(boolean unprotectingDataEnabled) {
        this.unprotectingDataEnabled = unprotectingDataEnabled;
    }
}
