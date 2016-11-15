package eu.clarussecure.proxy.protocol;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.StringUtilities;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;

public class ProtocolServiceDelegate implements ProtocolService {

    private ProtectionModule protectionModule;

    public ProtocolServiceDelegate(ProtectionModule protectionModule) {
        this.protectionModule = protectionModule;
    }

	@Override
	public CString newUserIdentification(CString user) {
		return user;
	}
	
    @Override
    public List<DataOperation> newDataOperation(DataOperation dataOperation) {
        Objects.requireNonNull(dataOperation);
        List<DataOperation> dataOperations;
        switch (dataOperation.getOperation()) {
        case CREATE:
            dataOperations = newCreateOperation(dataOperation);
            break;
        case READ:
            dataOperations = newReadOperation(dataOperation);
            break;
        case UPDATE:
        case DELETE:
        default:
            dataOperations = Collections.singletonList(dataOperation);
            break;
        }
        return dataOperations;
    }

    private List<DataOperation> newCreateOperation(DataOperation dataOperation) {
        String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        String[][] contents = dataOperation.getDataValues().stream().map(row -> row.stream().map(StringUtilities::toString).map(StringUtilities::unquote).toArray(String[]::new)).toArray(String[][]::new);
        // Protect data
        String[][] results = protectionModule.getDataOperation().post(attributeNames, contents);
        if (results != null && !Arrays.equals(results, contents)) {
            List<List<CString>> newDataValues = Arrays.stream(results).map(result -> Arrays.stream(result).map(StringUtilities::singleQuote).map(CString::valueOf).collect(Collectors.toList())).collect(Collectors.toList());
            dataOperation.setDataValues(newDataValues);
            dataOperation.setModified(true);
        }
        return Collections.singletonList(dataOperation);
    }

    private List<DataOperation> newReadOperation(DataOperation dataOperation) {
        String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        Promise promise = dataOperation.getPromise();
        if (promise == null) { // Request
            String[] criteriaOrig = null;
            String[] criteriaRes = null;
            if (dataOperation.getParameterValues() != null) {
                criteriaOrig = dataOperation.getParameterValues().stream().map(CString::toString).toArray(String[]::new);
                criteriaRes = criteriaOrig.clone();
            }
            promise = protectionModule.getDataOperation().get(attributeNames, criteriaRes, null);
            if (promise != null) {
                if (criteriaRes != null && !Arrays.equals(criteriaRes, criteriaOrig)) {
                    // TODO change Promise to handle modified criteria
                    List<CString> newParameterValues = Arrays.stream(criteriaRes).map(CString::valueOf).collect(Collectors.toList());
                    dataOperation.setParameterValues(newParameterValues);
                    dataOperation.setModified(true);
                }
                dataOperation.setPromise(promise);
            }
        } else { // Response
            String[][] contents = dataOperation.getDataValues().stream().map(row -> row.stream().map(StringUtilities::toString).toArray(String[]::new)).toArray(String[][]::new);
            String[][] results = protectionModule.getDataOperation().get(promise, contents);
            if (results != null && results != contents) {
                List<List<CString>> newDataValues = Arrays.stream(results).map(result -> Arrays.stream(result).map(CString::valueOf).collect(Collectors.toList())).collect(Collectors.toList());
                dataOperation.setDataValues(newDataValues);
                dataOperation.setModified(true);
            }
        }
        return Collections.singletonList(dataOperation);
    }


}
