package eu.clarussecure.proxy.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import eu.clarussecure.dataoperations.Criteria;
import eu.clarussecure.dataoperations.DataOperationCommand;
import eu.clarussecure.dataoperations.DataOperationResponse;
import eu.clarussecure.dataoperations.DataOperationResult;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;
import eu.clarussecure.proxy.spi.StringUtilities;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;

public class ProtocolServiceDelegate implements ProtocolService {

    private ProtectionModule protectionModule;

    public ProtocolServiceDelegate(ProtectionModule protectionModule) {
        this.protectionModule = protectionModule;
    }

    @Override
    public CString[] userAuthentication(CString user, CString password) {
        return new CString[] { user, password };
    }

    @Override
    public CString newUserIdentification(CString user) {
        return user;
    }

    @Override
    public MetadataOperation newMetadataOperation(MetadataOperation metadataOperation) {
        Objects.requireNonNull(metadataOperation);
        String[] attributeNames = metadataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        List<Map<String, String>> mapping = protectionModule.getDataOperation().head(attributeNames);
        if (mapping != null) {
            metadataOperation.getMetadata().clear();
            Function<Map.Entry<String, String>, String> getKey = Map.Entry::getKey;
            Function<Map.Entry<String, String>, String> getValue = Map.Entry::getValue;
            Function<String, CString> valueOf = CString::valueOf;
            Map<CString, List<CString>> metadata = mapping.stream().map(Map::entrySet).flatMap(Set::stream)
                    .collect(Collectors.groupingBy(getKey.andThen(valueOf), HashMap::new,
                            Collectors.mapping(getValue.andThen(valueOf), Collectors.toList())));
            metadataOperation.setMetadata(metadata.entrySet().stream().collect(Collectors.toList()));
            metadataOperation.setModified(true);
        }
        return metadataOperation;
    }

    @Override
    public List<DataOperation> newDataOperation(DataOperation dataOperation) {
        Objects.requireNonNull(dataOperation);
        List<DataOperation> dataOperations;
        if (dataOperation.getOperation() != null) {
            switch (dataOperation.getOperation()) {
            case CREATE:
                dataOperations = newCreateOperation(dataOperation);
                break;
            case READ:
                dataOperations = newReadOperation(dataOperation);
                break;
            case UPDATE:
                dataOperations = newUpdateOperation(dataOperation);
                break;
            case DELETE:
                dataOperations = newDeleteOperation(dataOperation);
                break;
            default:
                dataOperations = Collections.singletonList(dataOperation);
                break;
            }
        } else {
            dataOperations = broadcastOperation(dataOperation);
        }
        return dataOperations;
    }

    private List<DataOperation> newCreateOperation(DataOperation dataOperation) {
        String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        String[][] contents = dataOperation.getDataValues().stream().map(
                row -> row.stream().map(StringUtilities::toString).map(StringUtilities::unquote).toArray(String[]::new))
                .toArray(String[][]::new);
        List<DataOperationCommand> results = protectionModule.getDataOperation().post(attributeNames, contents);
        List<DataOperation> dataOperations = null;
        if (results != null && results.size() > 0) {
            String[][] newAttributeNames = results.stream().map(DataOperationCommand::getProtectedAttributeNames)
                    .toArray(String[][]::new);
            String[][][] newContents = results.stream().map(DataOperationCommand::getProtectedContents)
                    .toArray(String[][][]::new);
            if ((newAttributeNames.length > 1 || !Arrays.equals(attributeNames, newAttributeNames[0]))
                    || (newContents.length > 1 || !Arrays.equals(contents, newContents[0]))) {
                dataOperations = new ArrayList<>(results.size());
                for (int csp = 0; csp < results.size(); csp++) {
                    if (dataOperation.getInvolvedCSPs() != null && !dataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    if (newAttributeNames[csp].length == 0) {
                        continue;
                    }
                    DataOperation newDataOperation = new DataOperation();
                    newDataOperation.setAttributes(dataOperation.getAttributes());
                    newDataOperation.setRequestId(dataOperation.getRequestId());
                    newDataOperation.setOperation(dataOperation.getOperation());
                    newDataOperation.setUnprotectingDataEnabled(dataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = Arrays.stream(newAttributeNames[csp]).filter(an -> an != null)
                            .map(CString::valueOf).collect(Collectors.toList());
                    newDataOperation.setDataIds(dataIds);
                    List<List<CString>> newDataValues = Arrays.stream(newContents[csp]).map(row -> Arrays.stream(row)
                            .map(StringUtilities::singleQuote).map(CString::valueOf).collect(Collectors.toList()))
                            .collect(Collectors.toList());
                    newDataOperation.setDataValues(newDataValues);
                    if (!dataOperation.getParameterIds().isEmpty()) {
                        // replace each parameter id that is a clear attribute
                        // name by the protected attribute name
                        int csp2 = csp;
                        List<CString> parameterIds = dataOperation.getParameterIds().stream().map(pid -> {
                            String newParameterId = results.get(csp2).getMapping().get(pid.toString());
                            return newParameterId != null
                                    ? CString.valueOf(newParameterId.substring(newParameterId.indexOf('/') + 1)) : pid;
                        }).collect(Collectors.toList());
                        newDataOperation.setParameterIds(parameterIds);
                    }
                    newDataOperation.setParameterValues(dataOperation.getParameterValues());
                    newDataOperation.setModified(true);
                    newDataOperation.setInvolvedCSP(csp);
                    dataOperations.add(newDataOperation);
                }
            }
        }
        if (dataOperations == null) {
            dataOperations = Collections.singletonList(dataOperation);
        }
        return dataOperations;
    }

    private List<DataOperation> newReadOperation(DataOperation dataOperation) {
        List<DataOperationCommand> promise = dataOperation.getPromise();
        List<DataOperation> dataOperations;
        if (promise == null) { // Request
            String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
            Criteria[] criteria = IntStream.range(0, dataOperation.getParameterIds().size())
                    .mapToObj(i -> new Criteria(dataOperation.getParameterIds().get(i).toString(), "=clarus_equals=",
                            dataOperation.getParameterValues().get(i).toString()))
                    .toArray(Criteria[]::new);
            promise = protectionModule.getDataOperation().get(attributeNames,
                    criteria /*
                              * TODO dispatch, dataOperation.isModified() &&
                              * dataOperation.getInvolvedCSPs() != null
                              */);
            if (promise != null) {
                dataOperations = new ArrayList<>(promise.size());
                for (int csp = 0; csp < promise.size(); csp++) {
                    if (dataOperation.getInvolvedCSPs() != null && !dataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    DataOperationCommand cspPromise = promise.get(csp);
                    if (cspPromise.getProtectedAttributeNames().length == 0) {
                        continue;
                    }
                    DataOperation newDataOperation = new DataOperation();
                    newDataOperation.setAttributes(dataOperation.getAttributes());
                    newDataOperation.setRequestId(dataOperation.getRequestId());
                    newDataOperation.setOperation(dataOperation.getOperation());
                    newDataOperation.setUnprotectingDataEnabled(dataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = Arrays.stream(cspPromise.getProtectedAttributeNames()).map(CString::valueOf)
                            .collect(Collectors.toList());
                    newDataOperation.setDataIds(dataIds);
                    // TODO does criteria attribute names change ?
                    Criteria[] newCriteria = cspPromise.getCriteria();
                    List<CString> newParameterIds = Arrays.stream(newCriteria).map(Criteria::getAttributeName)
                            .map(CString::valueOf).collect(Collectors.toList());
                    List<CString> newParameterValues = Arrays.stream(newCriteria).map(Criteria::getValue)
                            .map(CString::valueOf).collect(Collectors.toList());
                    newDataOperation.setParameterIds(newParameterIds);
                    newDataOperation.setParameterValues(newParameterValues);
                    newDataOperation.setPromise(promise);
                    boolean modified = dataOperation.isModified();
                    if (!modified) {
                        modified = promise.size() > 1;
                    }
                    if (!modified) {
                        String[] attributeShortNames = Arrays.stream(cspPromise.getAttributeNames())
                                .map(an -> an.substring(an.indexOf('/') + 1)).toArray(String[]::new);
                        String[] protectedAttributeShortNames = Arrays.stream(cspPromise.getProtectedAttributeNames())
                                .map(pan -> pan.substring(pan.indexOf('/') + 1)).toArray(String[]::new);
                        modified = !Arrays.equals(attributeShortNames, protectedAttributeShortNames);
                    }
                    if (!modified) {
                        String[] criteriaShortNames = Arrays.stream(cspPromise.getCriteria())
                                .map(Criteria::getAttributeName).map(c -> c.substring(c.indexOf('/') + 1))
                                .toArray(String[]::new);
                        String[] protectedCriteriaShortNames = Arrays.stream(cspPromise.getCriteria())
                                .map(Criteria::getAttributeName).map(pc -> pc.substring(pc.indexOf('/') + 1))
                                .toArray(String[]::new);
                        modified = !Arrays.equals(criteriaShortNames, protectedCriteriaShortNames);
                    }
                    newDataOperation.setModified(modified);
                    newDataOperation.setInvolvedCSP(csp);
                    dataOperations.add(newDataOperation);
                }
            } else {
                dataOperations = Collections.singletonList(dataOperation);
            }
        } else { // Response
            List<String[][]> contents = dataOperation.getDataValues().stream()
                    .map(row -> row.stream().map(StringUtilities::toString).toArray(String[]::new))
                    .map(row -> new String[][] { row }).collect(Collectors.toList());
            List<DataOperationResult> results = protectionModule.getDataOperation().get(promise, contents);
            boolean same = true;
            if (contents.size() != 1) {
                same = false;
            } else if (results.size() == 1 && results.get(0) instanceof DataOperationResponse) {
                String[][] content = contents.get(0);
                String[][] newContent = ((DataOperationResponse) results.get(0)).getContents();
                if (content != newContent) {
                    if (content.length != newContent.length) {
                        same = false;
                    } else {
                        loop1: for (int i = 0; i < content.length; i++) {
                            if (content[i] != newContent[i]) {
                                if (content[i].length != newContent[i].length) {
                                    same = false;
                                    break loop1;
                                } else {
                                    for (int j = 0; j < content[i].length; j++) {
                                        if (content[i][j] != newContent[i][j]) {
                                            same = false;
                                            break loop1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!same && results.size() == 1 && results.get(0) instanceof DataOperationResponse) {
                String[][] newContent = ((DataOperationResponse) results.get(0)).getContents();
                List<List<CString>> newDataValues = Arrays.stream(newContent).map(row -> Arrays.stream(row).map(v -> {
                    CString dataValue = null;
                    boolean found = false;
                    loop1: for (int csp = 0; csp < contents.size(); csp++) {
                        String[][] cspContent = contents.get(csp);
                        for (int i = 0; i < cspContent[0].length; i++) {
                            if (v == cspContent[0][i]) {
                                dataValue = dataOperation.getDataValues().get(csp).get(i);
                                found = true;
                                break loop1;
                            }
                        }
                    }
                    if (!found) {
                        dataValue = CString.valueOf(v);
                    }
                    return dataValue;
                }).collect(Collectors.toList())).collect(Collectors.toList());
                dataOperation.setDataValues(newDataValues);
                dataOperation.setModified(true);
            }
            dataOperations = Collections.singletonList(dataOperation);
        }
        return dataOperations;
    }

    private List<DataOperation> newUpdateOperation(DataOperation dataOperation) {
        String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        Criteria[] criteria = IntStream.range(0, dataOperation.getParameterIds().size())
                .mapToObj(i -> new Criteria(dataOperation.getParameterIds().get(i).toString(), "=clarus_equals=",
                        dataOperation.getParameterValues().get(i).toString()))
                .toArray(Criteria[]::new);
        Criteria[] newCriteria = criteria.clone();
        String[][] contents = dataOperation.getDataValues().stream().map(
                row -> row.stream().map(StringUtilities::toString).map(StringUtilities::unquote).toArray(String[]::new))
                .toArray(String[][]::new);
        // Protect data
        List<DataOperationCommand> results = protectionModule.getDataOperation().put(attributeNames, newCriteria,
                contents);
        List<DataOperation> dataOperations = null;
        if (results != null && results.size() > 0) {
            String[][] newAttributeNames = results.stream().map(DataOperationCommand::getProtectedAttributeNames)
                    .toArray(String[][]::new);
            String[][][] newContents = results.stream().map(DataOperationCommand::getProtectedContents)
                    .toArray(String[][][]::new);
            if ((newAttributeNames.length > 1 || !Arrays.equals(attributeNames, newAttributeNames[0]))
                    || (newContents.length > 1 || !Arrays.equals(contents, newContents[0]))
                    || (newCriteria != null && !Arrays.equals(criteria, newCriteria))) {
                dataOperations = new ArrayList<>(results.size());
                for (int csp = 0; csp < results.size(); csp++) {
                    if (dataOperation.getInvolvedCSPs() != null && !dataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    if (newAttributeNames[csp].length == 0) {
                        continue;
                    }
                    DataOperation newDataOperation = new DataOperation();
                    newDataOperation.setAttributes(dataOperation.getAttributes());
                    newDataOperation.setRequestId(dataOperation.getRequestId());
                    newDataOperation.setOperation(dataOperation.getOperation());
                    newDataOperation.setUnprotectingDataEnabled(dataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = Arrays.stream(newAttributeNames[csp]).filter(an -> an != null)
                            .map(CString::valueOf).collect(Collectors.toList());
                    newDataOperation.setDataIds(dataIds);
                    List<List<CString>> newDataValues = Arrays.stream(newContents[csp]).map(row -> Arrays.stream(row)
                            .map(StringUtilities::singleQuote).map(CString::valueOf).collect(Collectors.toList()))
                            .collect(Collectors.toList());
                    newDataOperation.setDataValues(newDataValues);
                    // TODO does criteria attribute names change ?
                    List<CString> newParameterIds = Arrays.stream(newCriteria).map(Criteria::getAttributeName)
                            .map(CString::valueOf).collect(Collectors.toList());
                    List<CString> newParameterValues = Arrays.stream(newCriteria).map(Criteria::getValue)
                            .map(CString::valueOf).collect(Collectors.toList());
                    newDataOperation.setParameterIds(newParameterIds);
                    newDataOperation.setParameterValues(newParameterValues);
                    newDataOperation.setModified(true);
                    newDataOperation.setInvolvedCSP(csp);
                    dataOperations.add(newDataOperation);
                }
            }
        }
        if (dataOperations == null) {
            dataOperations = Collections.singletonList(dataOperation);
        }
        return dataOperations;
    }

    private List<DataOperation> newDeleteOperation(DataOperation dataOperation) {
        String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        Criteria[] criteria = IntStream.range(0, dataOperation.getParameterIds().size())
                .mapToObj(i -> new Criteria(dataOperation.getParameterIds().get(i).toString(), "=clarus_equals=",
                        dataOperation.getParameterValues().get(i).toString()))
                .toArray(Criteria[]::new);
        Criteria[] newCriteria = criteria.clone();
        // Protect data
        List<DataOperationCommand> results = protectionModule.getDataOperation().delete(attributeNames, newCriteria);
        List<DataOperation> dataOperations = null;
        if (results != null && results.size() > 0) {
            String[][] newAttributeNames = results.stream().map(DataOperationCommand::getProtectedAttributeNames)
                    .toArray(String[][]::new);
            if ((newAttributeNames.length > 1 || !Arrays.equals(attributeNames, newAttributeNames[0]))
                    || (newCriteria != null && !Arrays.equals(criteria, newCriteria))) {
                dataOperations = new ArrayList<>(results.size());
                for (int csp = 0; csp < results.size(); csp++) {
                    if (dataOperation.getInvolvedCSPs() != null && !dataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    if (newAttributeNames[csp].length == 0) {
                        continue;
                    }
                    DataOperation newDataOperation = new DataOperation();
                    newDataOperation.setAttributes(dataOperation.getAttributes());
                    newDataOperation.setRequestId(dataOperation.getRequestId());
                    newDataOperation.setOperation(dataOperation.getOperation());
                    newDataOperation.setUnprotectingDataEnabled(dataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = Arrays.stream(newAttributeNames[csp]).filter(an -> an != null)
                            .map(CString::valueOf).collect(Collectors.toList());
                    newDataOperation.setDataIds(dataIds);
                    // TODO does criteria attribute names change ?
                    List<CString> newParameterIds = Arrays.stream(newCriteria).map(Criteria::getAttributeName)
                            .map(CString::valueOf).collect(Collectors.toList());
                    List<CString> newParameterValues = Arrays.stream(newCriteria).map(Criteria::getValue)
                            .map(CString::valueOf).collect(Collectors.toList());
                    newDataOperation.setParameterIds(newParameterIds);
                    newDataOperation.setParameterValues(newParameterValues);
                    newDataOperation.setModified(true);
                    newDataOperation.setInvolvedCSP(csp);
                    dataOperations.add(newDataOperation);
                }
            }
        }
        if (dataOperations == null) {
            dataOperations = Collections.singletonList(dataOperation);
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

}
