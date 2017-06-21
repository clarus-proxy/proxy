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
    private List<CString> parameterIds;
    private List<CString> parameterValues;
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
        dataIds.forEach(dataId -> getDataIds().add(dataId));
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

    public void addDataValues(List<CString> dataValues) {
        getDataValues().add(dataValues);
    }

    public List<CString> getParameterIds() {
        if (parameterIds == null) {
            parameterIds = new ArrayList<>();
        }
        return parameterIds;
    }

    public void setParameterIds(List<CString> parameterIds) {
        this.parameterIds = parameterIds;
    }

    public void addParameterId(CString parameterId) {
        getParameterIds().add(parameterId);
    }

    public List<CString> getParameterValues() {
        if (parameterValues == null) {
            parameterValues = new ArrayList<>();
        }
        return parameterValues;
    }

    public void setParameterValues(List<CString> parameterValues) {
        this.parameterValues = parameterValues;
    }

    public void addParameterValue(CString parameterValue) {
        getParameterValues().add(parameterValue);
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
