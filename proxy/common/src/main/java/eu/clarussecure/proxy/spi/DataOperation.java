package eu.clarussecure.proxy.spi;

import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.dataoperations.Promise;

public class DataOperation extends ModuleOperation {
    private int requestId;
    private Operation operation;
    private List<CString> dataIds;
    private List<CString> parameterIds;
    private List<List<CString>> dataValues;
    private List<CString> parameterValues;
    private Promise promise;
    private boolean resultProcessingEnabled = true;
    private List<CString> involvedCSPs;

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
    public void removeDataId(CString dataId) {
        getDataIds().remove(dataId);
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

    public Promise getPromise() {
        return promise;
    }

    public void setPromise(Promise promise) {
        this.promise = promise;
    }

    public boolean isResultProcessingEnabled() {
        return resultProcessingEnabled;
    }

    public void setResultProcessingEnabled(boolean resultProcessingEnabled) {
        this.resultProcessingEnabled = resultProcessingEnabled;
    }

    public List<CString> getInvolvedCSPs() {
        return involvedCSPs;
    }

    public void setInvolvedCSPs(List<CString> involvedCSPs) {
        this.involvedCSPs = involvedCSPs;
    }
}
