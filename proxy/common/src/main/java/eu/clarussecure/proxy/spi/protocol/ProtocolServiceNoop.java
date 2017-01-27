package eu.clarussecure.proxy.spi.protocol;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;

public class ProtocolServiceNoop implements ProtocolService {

    @Override
    public MetadataOperation newMetadataOperation(MetadataOperation metadataOperation) {
        Objects.requireNonNull(metadataOperation);
        return metadataOperation;
    }

    @Override
    public List<DataOperation> newDataOperation(DataOperation dataOperation) {
        Objects.requireNonNull(dataOperation);
        List<DataOperation> dataOperations;
        if (dataOperation.getOperation() != null) {
            dataOperations = Collections.singletonList(dataOperation);
        } else {
            dataOperations = broadcastOperation(dataOperation);
        }
        return dataOperations;
    }

    private List<DataOperation> broadcastOperation(DataOperation dataOperation) {
        List<DataOperation> newDataOperations;
        if (dataOperation.getInvolvedCSPs() != null && dataOperation.getInvolvedCSPs().size() > 1) {
            newDataOperations = dataOperation.getInvolvedCSPs().stream().map(csp -> {
                DataOperation newDataOperation = new DataOperation();
                newDataOperation.setModified(true);
                newDataOperation.setAttributes(dataOperation.getAttributes());
                newDataOperation.setInvolvedCSP(csp);
                newDataOperation.setRequestId(dataOperation.getRequestId());
                newDataOperation.setOperation(dataOperation.getOperation());
                newDataOperation.setDataIds(dataOperation.getDataIds());
                newDataOperation.setParameterIds(dataOperation.getParameterIds());
                newDataOperation.setDataValues(dataOperation.getDataValues());
                newDataOperation.setParameterValues(dataOperation.getParameterValues());
                newDataOperation.setPromise(dataOperation.getPromise());
                newDataOperation.setUnprotectingDataEnabled(dataOperation.isUnprotectingDataEnabled());
                return newDataOperation;
            }).collect(Collectors.toList());
        } else {
            newDataOperations = Collections.singletonList(dataOperation);
        }
        return newDataOperations;
    }

    @Override
    public CString newUserIdentification(CString user) {
        return user;
    }

    @Override
    public CString[] userAuthentication(CString user, CString password) {
        return new CString[] { user, password };
    }
}
