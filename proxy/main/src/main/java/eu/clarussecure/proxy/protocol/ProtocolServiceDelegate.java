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
        String[][] contents = dataOperation.getDataValues().stream().map(row -> row.stream().map(CString::toString).map(StringUtilities::unquote).toArray(String[]::new)).toArray(String[][]::new);
        // Protect data
        String[][] results = protectionModule.getDataOperation().post(attributeNames, contents);
        if (results != null) {
            List<List<CString>> newDataValues = Arrays.stream(results).map(result -> Arrays.stream(result).map(StringUtilities::singleQuote).map(CString::valueOf).collect(Collectors.toList())).collect(Collectors.toList());
            dataOperation.setDataValues(newDataValues);
        }
        return Collections.singletonList(dataOperation);
    }

    private List<DataOperation> newReadOperation(DataOperation dataOperation) {
        String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        String[] criteria = dataOperation.getParameterValues().stream().map(CString::toString).toArray(String[]::new);
        Promise promise = protectionModule.getDataOperation().get(attributeNames, criteria, null);
        if (promise != null) {
            // TODO change Promise to handle modified criteria
            List<CString> newParameterValues = Arrays.stream(criteria).map(CString::valueOf).collect(Collectors.toList());
            dataOperation.setParameterValues(newParameterValues);
        }
        return Collections.singletonList(dataOperation);
    }

}
