package eu.clarussecure.proxy.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;
import eu.clarussecure.proxy.spi.StringUtilities;
import eu.clarussecure.proxy.spi.protection.DefaultPromise;
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
        String[][] mapping = protectionModule.getDataOperation().head(attributeNames);
        if (mapping != null) {
            metadataOperation.getMetadata().clear();
            for (int i = 0; i < mapping.length; i++) {
                CString attributeName = CString.valueOf(mapping[i][0]);
                List<CString> protectedAttributeNames = Arrays.stream(mapping[i], 1, mapping[i].length).distinct()
                        .map(CString::valueOf).collect(Collectors.toList());
                metadataOperation.addDataId(attributeName, protectedAttributeNames);
            }
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
        // Get the mapping
        String[][] mapping = protectionModule.getDataOperation().head(attributeNames);
        // Protect data
        String[][][] results = protectionModule.getDataOperation().post(attributeNames, contents);
        String[][] newAttributeNames = null;
        if (results != null && results.length > 0) {
            newAttributeNames = Arrays.stream(results).map(cspResults -> cspResults[0]).toArray(String[][]::new);
            results = Arrays.stream(results)
                    .map(cspResults -> Arrays.stream(cspResults, 1, cspResults.length).toArray(String[][]::new))
                    .toArray(String[][][]::new);
        }
        List<DataOperation> dataOperations;
        if ((newAttributeNames != null
                && (newAttributeNames.length > 1 || !Arrays.equals(attributeNames, newAttributeNames[0])))
                || (results != null && (results.length > 1 || !Arrays.equals(contents, results[0])))) {
            dataOperations = new ArrayList<>(results.length);
            for (int csp = 0; csp < results.length; csp++) {
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
                List<List<CString>> newDataValues = Arrays.stream(results[csp]).map(result -> Arrays.stream(result)
                        .map(StringUtilities::singleQuote).map(CString::valueOf).collect(Collectors.toList()))
                        .collect(Collectors.toList());
                newDataOperation.setDataValues(newDataValues);
                if (!dataOperation.getParameterIds().isEmpty()) {
                    // replace each parameter id that is a clear attribute name
                    // by the protected attribute name
                    int csp2 = csp;
                    List<CString> parameterIds = dataOperation.getParameterIds().stream().map(pid -> {
                        String[] pMapping = Arrays.stream(mapping).filter(m -> pid.equalsIgnoreCase(m[0])).findAny()
                                .orElse(null);
                        return pMapping != null
                                ? CString.valueOf(pMapping[csp2 + 1].substring(pMapping[csp2 + 1].indexOf('/') + 1))
                                : pid;
                    }).collect(Collectors.toList());
                    newDataOperation.setParameterIds(parameterIds);
                }
                newDataOperation.setParameterValues(dataOperation.getParameterValues());
                newDataOperation.setModified(true);
                newDataOperation.setInvolvedCSP(csp);
                dataOperations.add(newDataOperation);
            }
        } else {
            dataOperations = Collections.singletonList(dataOperation);
        }
        return dataOperations;
    }

    private List<DataOperation> newReadOperation(DataOperation dataOperation) {
        DefaultPromise promise = (DefaultPromise) dataOperation.getPromise();
        List<DataOperation> dataOperations;
        if (promise == null) { // Request
            String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
            String[] criteria = IntStream.range(0, dataOperation.getParameterIds().size())
                    .mapToObj(i -> dataOperation.getParameterIds().get(i).toString() + "=clarus_equals="
                            + dataOperation.getParameterValues().get(i).toString())
                    .toArray(String[]::new);
            promise = (DefaultPromise) protectionModule.getDataOperation().get(attributeNames, criteria, null,
                    dataOperation.isModified()/* && !dataOperation.isUnprotectingDataEnabled()*/ && dataOperation.getInvolvedCSPs() != null);
            if (promise != null) {
                dataOperations = new ArrayList<>(promise.getProtectedAttributeNames().length);
                for (int csp = 0; csp < promise.getProtectedAttributeNames().length; csp++) {
                    if (dataOperation.getInvolvedCSPs() != null && !dataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    if (promise.getProtectedAttributeNames()[csp].length == 0) {
                        continue;
                    }
                    DataOperation newDataOperation = new DataOperation();
                    newDataOperation.setAttributes(dataOperation.getAttributes());
                    newDataOperation.setRequestId(dataOperation.getRequestId());
                    newDataOperation.setOperation(dataOperation.getOperation());
                    newDataOperation.setUnprotectingDataEnabled(dataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = Arrays.stream(promise.getProtectedAttributeNames()[csp])
                            .map(CString::valueOf).collect(Collectors.toList());
                    newDataOperation.setDataIds(dataIds);
                    if (csp < promise.getProtectedCriteria().length) {
                        String[] newCriteria = promise.getProtectedCriteria()[csp];
                        List<String[]> newParameters = Arrays.stream(newCriteria).map(c -> c.split("=clarus_equals="))
                                .collect(Collectors.toList());
                        List<CString> newParameterIds = newParameters.stream().map(tk -> tk[0]).map(CString::valueOf)
                                .collect(Collectors.toList());
                        List<CString> newParameterValues = newParameters.stream().map(tk -> tk[1]).map(CString::valueOf)
                                .collect(Collectors.toList());
                        newDataOperation.setParameterIds(newParameterIds);
                        newDataOperation.setParameterValues(newParameterValues);
                    }
                    newDataOperation.setPromise(promise);
                    boolean modified = dataOperation.isModified();
                    if (!modified) {
                        modified = promise.getProtectedAttributeNames().length > 1
                                || promise.getProtectedCriteria().length > 1;
                    }
                    if (!modified && promise.getProtectedAttributeNames().length > 0) {
                        String[] attributeShortNames = Arrays.stream(promise.getAttributeNames())
                                .map(an -> an.substring(an.indexOf('/') + 1)).toArray(String[]::new);
                        String[] protectedAttributeShortNames = Arrays.stream(promise.getProtectedAttributeNames()[0])
                                .map(pan -> pan.substring(pan.indexOf('/') + 1)).toArray(String[]::new);
                        modified = !Arrays.equals(attributeShortNames, protectedAttributeShortNames);
                    }
                    if (!modified && promise.getProtectedCriteria().length > 0) {
                        String[] criteriaShortNames = Arrays.stream(promise.getCriteria())
                                .map(c -> c.substring(c.indexOf('/') + 1)).toArray(String[]::new);
                        String[] protectedCriteriaShortNames = Arrays.stream(promise.getProtectedCriteria()[0])
                                .map(pc -> pc.substring(pc.indexOf('/') + 1)).toArray(String[]::new);
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
            String[][] contents = dataOperation.getDataValues().stream()
                    .map(row -> row.stream().map(StringUtilities::toString).toArray(String[]::new))
                    .toArray(String[][]::new);
            String[][] results = protectionModule.getDataOperation().get(promise, contents);
            boolean same = true;
            if (contents != results) {
                if (contents.length != results.length) {
                    same = false;
                } else {
                    loop1: for (int i = 0; i < contents.length; i++) {
                        if (contents[i] != results[i]) {
                            if (contents[i].length != results[i].length) {
                                same = false;
                                break loop1;
                            } else {
                                for (int j = 0; j < contents[i].length; j++) {
                                    if (contents[i][j] != results[i][j]) {
                                        same = false;
                                        break loop1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!same) {
                List<List<CString>> newDataValues = Arrays.stream(results)
                        .map(result -> Arrays.stream(result).map(r -> {
                            CString dataValue = null;
                            boolean found = false;
                            loop1: for (int i = 0; i < contents.length; i++) {
                                for (int j = 0; j < contents[i].length; j++) {
                                    if (r == contents[i][j]) {
                                        dataValue = dataOperation.getDataValues().get(i).get(j);
                                        found = true;
                                        break loop1;
                                    }
                                }
                            }
                            if (!found) {
                                dataValue = CString.valueOf(r);
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
        String[] criteria = IntStream.range(0, dataOperation.getParameterIds().size())
                .mapToObj(i -> dataOperation.getParameterIds().get(i).toString() + "=clarus_equals="
                        + dataOperation.getParameterValues().get(i).toString())
                .toArray(String[]::new);
        String[] newCriteria = criteria.clone();
        String[][] contents = dataOperation.getDataValues().stream().map(
                row -> row.stream().map(StringUtilities::toString).map(StringUtilities::unquote).toArray(String[]::new))
                .toArray(String[][]::new);
        // Protect data
        String[][][] results = protectionModule.getDataOperation().put(attributeNames, newCriteria, contents);
        String[][] newAttributeNames = null;
        if (results != null && results.length > 0) {
            newAttributeNames = Arrays.stream(results).map(cspResults -> cspResults[0]).toArray(String[][]::new);
            results = Arrays.stream(results)
                    .map(cspResults -> Arrays.stream(cspResults, 1, cspResults.length).toArray(String[][]::new))
                    .toArray(String[][][]::new);
        }
        List<DataOperation> dataOperations;
        if ((newAttributeNames != null
                && (newAttributeNames.length > 1 || !Arrays.equals(attributeNames, newAttributeNames[0])))
                || (newCriteria != null && !Arrays.equals(criteria, newCriteria))
                || (results != null && (results.length > 1 || !Arrays.equals(contents, results[0])))) {
            dataOperations = new ArrayList<>(results.length);
            for (int csp = 0; csp < results.length; csp++) {
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
                List<List<CString>> newDataValues = Arrays.stream(results[csp]).map(result -> Arrays.stream(result)
                        .map(StringUtilities::singleQuote).map(CString::valueOf).collect(Collectors.toList()))
                        .collect(Collectors.toList());
                newDataOperation.setDataValues(newDataValues);
                List<String[]> newParameters = Arrays.stream(newCriteria).map(c -> c.split("=clarus_equals="))
                        .collect(Collectors.toList());
                List<CString> newParameterIds = newParameters.stream().map(tk -> tk[0]).map(CString::valueOf)
                        .collect(Collectors.toList());
                List<CString> newParameterValues = newParameters.stream().map(tk -> tk[1]).map(CString::valueOf)
                        .collect(Collectors.toList());
                newDataOperation.setParameterIds(newParameterIds);
                newDataOperation.setParameterValues(newParameterValues);
                newDataOperation.setModified(true);
                newDataOperation.setInvolvedCSP(csp);
                dataOperations.add(newDataOperation);
            }
        } else {
            dataOperations = Collections.singletonList(dataOperation);
        }
        return dataOperations;
    }

    private List<DataOperation> newDeleteOperation(DataOperation dataOperation) {
        String[] attributeNames = dataOperation.getDataIds().stream().map(CString::toString).toArray(String[]::new);
        String[] criteria = IntStream.range(0, dataOperation.getParameterIds().size())
                .mapToObj(i -> dataOperation.getParameterIds().get(i).toString() + "=clarus_equals="
                        + dataOperation.getParameterValues().get(i).toString())
                .toArray(String[]::new);
        String[] newCriteria = criteria.clone();
        // Protect data
        String[][] newAttributeNames = protectionModule.getDataOperation().delete(attributeNames, newCriteria);
        List<DataOperation> dataOperations;
        if ((newAttributeNames != null
                && (newAttributeNames.length > 1 || !Arrays.equals(attributeNames, newAttributeNames[0])))
                || (newCriteria != null && !Arrays.equals(criteria, newCriteria))) {
            dataOperations = new ArrayList<>(newAttributeNames.length);
            for (int csp = 0; csp < newAttributeNames.length; csp++) {
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
                List<String[]> newParameters = Arrays.stream(newCriteria).map(c -> c.split("=clarus_equals="))
                        .collect(Collectors.toList());
                List<CString> newParameterIds = newParameters.stream().map(tk -> tk[0]).map(CString::valueOf)
                        .collect(Collectors.toList());
                List<CString> newParameterValues = newParameters.stream().map(tk -> tk[1]).map(CString::valueOf)
                        .collect(Collectors.toList());
                newDataOperation.setParameterIds(newParameterIds);
                newDataOperation.setParameterValues(newParameterValues);
                newDataOperation.setModified(true);
                newDataOperation.setInvolvedCSP(csp);
                dataOperations.add(newDataOperation);
            }
        } else {
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
