package eu.clarussecure.proxy.protocol;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import eu.clarussecure.dataoperations.Criteria;
import eu.clarussecure.dataoperations.DataOperationCommand;
import eu.clarussecure.dataoperations.DataOperationResponse;
import eu.clarussecure.dataoperations.DataOperationResult;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.InboundDataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;
import eu.clarussecure.proxy.spi.OutboundDataOperation;
import eu.clarussecure.proxy.spi.OutboundDataOperation.Criterion;
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
        List<Map<String, String>> mapping = head(attributeNames);
        List<String> moduleFQAttributeNames = mapping.stream().map(Map::keySet).flatMap(Set::stream).distinct()
                .collect(Collectors.toList());
        String[] fqAttributeNames = Arrays.stream(attributeNames).flatMap(attributeName -> {
            Stream<String> stream = Stream.of(attributeName);
            if (attributeName.indexOf('*') != -1) {
                Pattern pattern = Pattern.compile(escapeRegex(attributeName));
                if (moduleFQAttributeNames.stream().anyMatch(fqan -> pattern.matcher(fqan).matches())) {
                    stream = moduleFQAttributeNames.stream().filter(fqan -> pattern.matcher(fqan).matches());
                }
            }
            return stream;
        }).toArray(String[]::new);
        // add the additional attributes that the protection module has added
        Stream<String> additionalAttributeNames = moduleFQAttributeNames.stream()
                .filter(mfqan -> !Arrays.asList(fqAttributeNames).contains(mfqan));
        metadataOperation.getMetadata().clear();
        Map<CString, List<CString>> metadata = Stream.concat(Arrays.stream(fqAttributeNames), additionalAttributeNames)
                .distinct()
                .collect(Collectors.toMap(CString::valueOf,
                        an -> mapping.stream().anyMatch(map -> map.containsKey(an)) ? mapping.stream()
                                .map(map -> map.get(an)).map(CString::valueOf).collect(Collectors.toList())
                                : Collections.<CString>emptyList()));
        metadataOperation.setMetadata(metadata.entrySet().stream().collect(Collectors.toList()));
        metadataOperation.setInvolvedCSPs(
                IntStream.range(0, mapping.size()).mapToObj(Integer::valueOf).collect(Collectors.toList()));
        metadataOperation.setModified(true);
        return metadataOperation;
    }

    @Override
    public List<OutboundDataOperation> newOutboundDataOperation(OutboundDataOperation outboundDataOperation) {
        Objects.requireNonNull(outboundDataOperation);
        List<OutboundDataOperation> newOutboundDataOperations;
        if (outboundDataOperation.getOperation() != null) {
            switch (outboundDataOperation.getOperation()) {
            case CREATE:
                newOutboundDataOperations = newCreateOperation(outboundDataOperation);
                break;
            case READ:
                newOutboundDataOperations = newOutboundReadOperation(outboundDataOperation);
                break;
            case UPDATE:
                newOutboundDataOperations = newUpdateOperation(outboundDataOperation);
                break;
            case DELETE:
                newOutboundDataOperations = newDeleteOperation(outboundDataOperation);
                break;
            default:
                newOutboundDataOperations = Collections.singletonList(outboundDataOperation);
                break;
            }
        } else {
            newOutboundDataOperations = broadcastOutboundOperation(outboundDataOperation);
        }
        return newOutboundDataOperations;
    }

    @Override
    public InboundDataOperation newInboundDataOperation(List<InboundDataOperation> inboundDataOperations) {
        Objects.requireNonNull(inboundDataOperations);
        InboundDataOperation newInboundDataOperation = null;
        if (inboundDataOperations.stream().map(InboundDataOperation::getOperation).distinct().count() == 1) {
            if (inboundDataOperations.get(0).getOperation() != null) {
                switch (inboundDataOperations.get(0).getOperation()) {
                case READ:
                    newInboundDataOperation = newInboundReadOperation(inboundDataOperations);
                    break;
                default:
                    newInboundDataOperation = inboundDataOperations.get(0);
                    break;
                }
            } else {
                newInboundDataOperation = inboundDataOperations.get(0);
            }
        }
        return newInboundDataOperation;
    }

    private List<OutboundDataOperation> newCreateOperation(OutboundDataOperation outboundDataOperation) {
        List<OutboundDataOperation> newOutboundDataOperations = null;
        // Filter attribute names according to the mapping
        // (in order to pass only attributes that have to be protected)
        String[] allAttributeNames = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .toArray(String[]::new);
        List<Map<String, String>> mappings = head(allAttributeNames);
        int numberOfCSPs = mappings.size();
        List<Boolean> protectedAttributes = Arrays.stream(allAttributeNames)
                .map(an -> mappings.stream().anyMatch(map -> map.containsKey(an))).collect(Collectors.toList());
        String[] attributeNames = IntStream.range(0, allAttributeNames.length).filter(i -> protectedAttributes.get(i))
                .mapToObj(i -> allAttributeNames[i]).toArray(String[]::new);
        if (attributeNames.length > 0) {
            // at least one attribute has to be protected
            String[][] allContents;
            String[][] contents;
            String[][] newAttributeNames;
            String[][][] newContents;
            List<Map<String, Integer>> mappingIndexes;
            String[][] newExtraProtectedAttributeNames;
            InputStream[][] newExtraBinaryContents;
            allContents = outboundDataOperation.getDataValues().stream()
                    .map(row -> row.stream().map(StringUtilities::toString).toArray(String[]::new))
                    .toArray(String[][]::new);
            contents = Arrays.stream(allContents)
                    .map(allRow -> IntStream.range(0, protectedAttributes.size())
                            .filter(i -> protectedAttributes.get(i)).mapToObj(i -> allRow[i]).toArray(String[]::new))
                    .toArray(String[][]::new);
            if (outboundDataOperation.isUsingHeadOperation()) {
                // using head operation is more efficient when there is no
                // content to protect or content is not to protect
                newAttributeNames = mappings
                        .stream().map(
                                map -> Stream
                                        .concat(Arrays.stream(attributeNames).filter(an -> map.containsKey(an))
                                                .map(an -> map.get(an)),
                                                map.entrySet().stream()
                                                        .filter(e -> !Arrays.asList(attributeNames)
                                                                .contains(e.getKey()))
                                                        .map(Map.Entry::getValue))
                                        .toArray(String[]::new))
                        .toArray(String[][]::new);
                newContents = mappings.stream()
                        .map(map -> Arrays.stream(contents)
                                .map(row -> IntStream.range(0, attributeNames.length)
                                        .filter(c -> map.containsKey(attributeNames[c])).mapToObj(c -> row[c])
                                        .toArray(String[]::new))
                                .toArray(String[][]::new))
                        .toArray(String[][][]::new);
                mappingIndexes = IntStream
                        .range(0,
                                mappings.size())
                        .mapToObj(
                                csp -> mappings.get(csp).entrySet().stream()
                                        .collect(
                                                Collectors.toMap(Map.Entry::getKey,
                                                        e -> IntStream.range(0, newAttributeNames[csp].length)
                                                                .filter(c -> newAttributeNames[csp][c]
                                                                        .equals(e.getValue()))
                                                                .findFirst().getAsInt())))
                        .collect(Collectors.toList());
                newExtraProtectedAttributeNames = new String[numberOfCSPs][0];
                newExtraBinaryContents = new InputStream[numberOfCSPs][0];
            } else {
                // using post operation allow to protect content
                List<DataOperationCommand> results = post(attributeNames, contents);
                if (results == null) {
                    newAttributeNames = Stream.<String[]>generate(() -> attributeNames).limit(numberOfCSPs)
                            .toArray(String[][]::new);
                    newContents = new String[numberOfCSPs][0][];
                    mappingIndexes = Stream.<Map<String, Integer>>generate(() -> Collections.emptyMap())
                            .limit(numberOfCSPs).collect(Collectors.toList());
                    newExtraProtectedAttributeNames = new String[numberOfCSPs][0];
                    newExtraBinaryContents = new InputStream[numberOfCSPs][0];
                } else {
                    newAttributeNames = results.stream().map(DataOperationCommand::getProtectedAttributeNames)
                            .map(pans -> pans == null ? new String[0] : pans).toArray(String[][]::new);
                    newContents = results.stream().map(DataOperationCommand::getProtectedContents)
                            .toArray(String[][][]::new);
                    mappingIndexes = results.stream()
                            .map(result -> result.getMapping().entrySet().stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                            e -> IntStream.range(0, result.getProtectedAttributeNames().length)
                                                    .filter(i -> result.getProtectedAttributeNames()[i]
                                                            .equals(e.getValue()))
                                                    .findFirst().getAsInt())))
                            .collect(Collectors.toList());
                    newExtraProtectedAttributeNames = results.stream()
                            .map(DataOperationCommand::getExtraProtectedAttributeNames)
                            .map(epans -> epans == null ? new String[0] : epans).toArray(String[][]::new);
                    newExtraBinaryContents = results.stream().map(DataOperationCommand::getExtraBinaryContent)
                            .map(ebcs -> ebcs == null ? new InputStream[0] : ebcs).toArray(InputStream[][]::new);
                }
            }
            if (numberOfCSPs > 1 // more than one csp
                    // one csp but attribute names change
                    || (numberOfCSPs == 1
                            && !mappings.get(0).entrySet().stream().allMatch(e -> e.getKey().equals(e.getValue())))
                    // one csp but content change
                    || (newContents.length == 1 && !Arrays.equals(contents, newContents[0]))) {
                newOutboundDataOperations = new ArrayList<>(numberOfCSPs);
                for (int csp = 0; csp < numberOfCSPs; csp++) {
                    if (outboundDataOperation.getInvolvedCSPs() != null
                            && !outboundDataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    Map<String, String> cspMapping = mappings.get(csp);
                    if (cspMapping.size() == 0) {
                        continue;
                    }
                    Map<String, Integer> cspMappingIndexes = mappingIndexes.get(csp);
                    OutboundDataOperation newOutboundDataOperation = new OutboundDataOperation();
                    newOutboundDataOperation.setAttributes(outboundDataOperation.getAttributes());
                    newOutboundDataOperation.setRequestId(outboundDataOperation.getRequestId());
                    newOutboundDataOperation.setOperation(outboundDataOperation.getOperation());
                    newOutboundDataOperation.setUsingHeadOperation(outboundDataOperation.isUsingHeadOperation());
                    newOutboundDataOperation
                            .setUnprotectingDataEnabled(outboundDataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = new ArrayList<>(protectedAttributes.size());
                    String[] cspNewAttributeNames = newAttributeNames[csp];
                    for (int c = 0; c < protectedAttributes.size(); c++) {
                        if (protectedAttributes.get(c)) {
                            String newAttributeName = cspMapping.get(allAttributeNames[c]);
                            if (newAttributeName != null && Arrays.stream(cspNewAttributeNames)
                                    .anyMatch(nan -> nan.equals(newAttributeName))) {
                                dataIds.add(CString.valueOf(newAttributeName));
                            }
                        } else {
                            dataIds.add(CString.valueOf(allAttributeNames[c]));
                        }
                    }
                    // add additional attributes
                    List<Integer> newAttributeNamesIndex = Arrays.stream(cspNewAttributeNames).map(CString::valueOf)
                            .map(nan -> dataIds.indexOf(nan)).collect(Collectors.toList());
                    for (int c = 0; c < newAttributeNamesIndex.size(); c++) {
                        if (newAttributeNamesIndex.get(c) == -1) {
                            dataIds.add(CString.valueOf(cspNewAttributeNames[c]));
                        }
                    }
                    newOutboundDataOperation.setDataIds(dataIds);
                    String[][] cspNewContents = newContents[csp];
                    List<List<CString>> newDataValues = new ArrayList<>(allContents.length);
                    for (int r = 0; r < allContents.length; r++) {
                        List<CString> rowDataValues = new ArrayList<>(protectedAttributes.size());
                        for (int c = 0; c < protectedAttributes.size(); c++) {
                            if (protectedAttributes.get(c)) {
                                String newAttributeName = cspMapping.get(allAttributeNames[c]);
                                if (newAttributeName != null && Arrays.stream(cspNewAttributeNames)
                                        .anyMatch(nan -> nan.equals(newAttributeName))) {
                                    rowDataValues.add(CString
                                            .valueOf(cspNewContents[r][cspMappingIndexes.get(allAttributeNames[c])]));
                                }
                            } else {
                                rowDataValues.add(CString.valueOf(allContents[r][c]));
                            }
                        }
                        // add additional data values
                        for (int c = 0; c < newAttributeNamesIndex.size(); c++) {
                            if (newAttributeNamesIndex.get(c) == -1) {
                                rowDataValues.add(CString.valueOf(cspNewContents[r][c]));
                            }
                        }
                        newDataValues.add(rowDataValues);
                    }
                    newOutboundDataOperation.setDataValues(newDataValues);
                    Map<CString, CString> dataIdMapping = cspMapping.entrySet().stream().collect(
                            Collectors.toMap(e -> CString.valueOf(e.getKey()), e -> CString.valueOf(e.getValue())));
                    newOutboundDataOperation.setDataIdMapping(dataIdMapping);
                    // Add extra protected attribute names
                    String[] cspNewExtraProtectedAttributeNames = newExtraProtectedAttributeNames[csp];
                    List<CString> newExtraDataIds = Arrays.stream(cspNewExtraProtectedAttributeNames)
                            .map(CString::valueOf).collect(Collectors.toList());
                    newOutboundDataOperation.setExtraDataIds(newExtraDataIds);
                    InputStream[] cspNewExtraBinaryContents = newExtraBinaryContents[csp];
                    List<InputStream> newExtraDataContents = Arrays.stream(cspNewExtraBinaryContents)
                            .collect(Collectors.toList());
                    newOutboundDataOperation.setExtraDataContents(newExtraDataContents);
                    newOutboundDataOperation.setModified(true);
                    newOutboundDataOperation.setInvolvedCSP(csp);
                    newOutboundDataOperations.add(newOutboundDataOperation);
                }
            }
        }
        if (newOutboundDataOperations == null) {
            // nothing to protect
            if (outboundDataOperation.getInvolvedCSPs() != null) {
                // retain only the first CSP
                outboundDataOperation.getInvolvedCSPs().removeIf(csp -> csp != 0);
            }
            if (outboundDataOperation.getInvolvedCSPs() == null || !outboundDataOperation.getInvolvedCSPs().isEmpty()) {
                Map<CString, CString> dataIdMapping = outboundDataOperation.getDataIds().stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
                outboundDataOperation.setDataIdMapping(dataIdMapping);
                outboundDataOperation.setInvolvedCSP(0);
                newOutboundDataOperations = Collections.singletonList(outboundDataOperation);
            } else {
                newOutboundDataOperations = Collections.emptyList();
            }
        }
        return newOutboundDataOperations;
    }

    private List<OutboundDataOperation> newOutboundReadOperation(OutboundDataOperation outboundDataOperation) {
        if (outboundDataOperation.isUsingHeadOperation()) {
            // using head operation is not supported (because of the
            // promise)
            throw new IllegalArgumentException("Read operation cannot rely only on the head operation");
        }
        List<OutboundDataOperation> newOutboundDataOperations = null;
        // Filter attribute names according to the mapping
        // (in order to pass only attributes that have to be protected)
        String[] allAttributeNames = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .toArray(String[]::new);
        List<Criterion> allCriterions = outboundDataOperation.getCriterions();
        String[] allCriteriaAttributeNames = allCriterions.stream()
                .map(criterion -> criterion != null ? criterion.getDataId().toString() : null).toArray(String[]::new);
        List<Map<String, String>> mappings = head(Stream
                .concat(Stream.of(allAttributeNames), Stream.of(allCriteriaAttributeNames).filter(can -> can != null))
                .toArray(String[]::new));
        int numberOfCSPs = mappings.size();
        List<String> fqAttributeNames = mappings.stream().map(Map::keySet).flatMap(Set::stream).distinct()
                .filter(fqan -> {
                    String an = fqan.substring(fqan.lastIndexOf('/') + 1);
                    return an.charAt(0) != '?' || an.charAt(an.length() - 1) != '?';
                }).collect(Collectors.toList());
        String[] allFQAttributeNames = Arrays.stream(allAttributeNames).flatMap(attributeName -> {
            Stream<String> stream = Stream.of(attributeName);
            if (attributeName.indexOf('*') != -1) {
                Pattern pattern = Pattern.compile(escapeRegex(attributeName));
                if (fqAttributeNames.stream().anyMatch(fqan -> pattern.matcher(fqan).matches())) {
                    stream = fqAttributeNames.stream().filter(fqan -> pattern.matcher(fqan).matches());
                }
            }
            return stream;
        }).toArray(String[]::new);
        List<Boolean> protectedAttributes = Arrays.stream(allFQAttributeNames)
                .map(an -> mappings.stream().anyMatch(map -> map.containsKey(an))).collect(Collectors.toList());
        String[] attributeNames = IntStream.range(0, allFQAttributeNames.length).filter(i -> protectedAttributes.get(i))
                .mapToObj(i -> allFQAttributeNames[i]).distinct().toArray(String[]::new);
        CString[][] allDataValues = outboundDataOperation.getDataValues().stream()
                .map(row -> row.stream().toArray(CString[]::new)).toArray(CString[][]::new);
        CString[][] allFQDataValues = Arrays.stream(allDataValues)
                .map(row -> IntStream.range(0, allAttributeNames.length).flatMap(c -> {
                    IntStream stream = IntStream.of(c);
                    if (allAttributeNames[c].indexOf('*') != -1) {
                        Pattern pattern = Pattern.compile(escapeRegex(allAttributeNames[c]));
                        if (fqAttributeNames.stream().anyMatch(fqan -> pattern.matcher(fqan).matches())) {
                            stream = IntStream.generate(() -> c).limit(
                                    fqAttributeNames.stream().filter(fqan -> pattern.matcher(fqan).matches()).count());
                        }
                    }
                    return stream;
                }).mapToObj(c -> row[c]).toArray(CString[]::new)).toArray(CString[][]::new);
        List<Boolean> dataValuesToProcess = IntStream.range(0, allFQAttributeNames.length)
                .mapToObj(c -> protectedAttributes.get(c)
                        && Arrays.asList(allFQAttributeNames).indexOf(allFQAttributeNames[c]) == c)
                .collect(Collectors.toList());
        Criteria[] allCriteria = allCriterions.stream()
                .map(criterion -> criterion != null ? new Criteria(criterion.getDataId().toString(),
                        criterion.getOperator().toString(), criterion.getValue().toString()) : null)
                .toArray(Criteria[]::new);
        List<Boolean> protectedCriteria = Arrays.stream(allCriteriaAttributeNames)
                .map(can -> can != null && mappings.stream().anyMatch(map -> map.containsKey(can)))
                .collect(Collectors.toList());
        Criteria[] criteria = IntStream.range(0, allCriteria.length).filter(i -> protectedCriteria.get(i))
                .mapToObj(i -> allCriteria[i]).toArray(Criteria[]::new);
        if (attributeNames.length > 0 || criteria.length > 0) {
            // at least one attribute or criteria has to be protected
            List<DataOperationCommand> promise = get(attributeNames, criteria);
            String[][] newAttributeNames = promise == null
                    ? Stream.<String[]>generate(() -> attributeNames).limit(numberOfCSPs).toArray(String[][]::new)
                    : promise.stream().map(DataOperationCommand::getProtectedAttributeNames)
                            .map(pans -> pans == null ? new String[0] : pans).toArray(String[][]::new);
            Criteria[][] newCriteria = promise == null
                    ? Stream.<Criteria[]>generate(() -> criteria).limit(numberOfCSPs).toArray(Criteria[][]::new)
                    : promise.stream().map(DataOperationCommand::getCriteria)
                            .map(cs -> cs == null ? new Criteria[0] : cs).toArray(Criteria[][]::new);
            if (numberOfCSPs > 1 // more than one csp
                    // one csp but attribute names change
                    || (numberOfCSPs == 1 && !mappings.get(0).entrySet().stream()
                            .allMatch(e -> e.getValue() == null || e.getKey().equals(e.getValue())))
                    // one csp but criteria change
                    || (newCriteria.length == 1 && !Arrays.equals(criteria, newCriteria[0]))) {
                newOutboundDataOperations = new ArrayList<>(numberOfCSPs);
                for (int csp = 0; csp < numberOfCSPs; csp++) {
                    if (outboundDataOperation.getInvolvedCSPs() != null
                            && !outboundDataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    Map<String, String> cspMapping = mappings.get(csp);
                    if (cspMapping.size() == 0 || newAttributeNames[csp].length == 0) {
                        continue;
                    }
                    OutboundDataOperation newOutboundDataOperation = new OutboundDataOperation();
                    newOutboundDataOperation.setAttributes(outboundDataOperation.getAttributes());
                    newOutboundDataOperation.setRequestId(outboundDataOperation.getRequestId());
                    newOutboundDataOperation.setOperation(outboundDataOperation.getOperation());
                    newOutboundDataOperation.setUsingHeadOperation(outboundDataOperation.isUsingHeadOperation());
                    newOutboundDataOperation
                            .setUnprotectingDataEnabled(outboundDataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = new ArrayList<>(protectedAttributes.size());
                    String[] cspNewAttributeNames = newAttributeNames[csp];
                    for (int c = 0; c < protectedAttributes.size(); c++) {
                        if (protectedAttributes.get(c)) {
                            String newAttributeName = cspMapping.get(allFQAttributeNames[c]);
                            if (newAttributeName != null && Arrays.stream(cspNewAttributeNames)
                                    .anyMatch(nan -> nan.equals(newAttributeName))) {
                                dataIds.add(CString.valueOf(newAttributeName));
                            }
                        } else {
                            dataIds.add(CString.valueOf(allFQAttributeNames[c]));
                        }
                    }
                    // add additional attributes
                    List<Integer> newAttributeNamesIndex = Arrays.stream(cspNewAttributeNames).map(CString::valueOf)
                            .map(nan -> dataIds.indexOf(nan)).collect(Collectors.toList());
                    for (int c = 0; c < newAttributeNamesIndex.size(); c++) {
                        if (newAttributeNamesIndex.get(c) == -1) {
                            dataIds.add(CString.valueOf(cspNewAttributeNames[c]));
                        }
                    }
                    newOutboundDataOperation.setDataIds(dataIds);
                    List<List<CString>> newDataValues = new ArrayList<>(allFQDataValues.length);
                    for (int r = 0; r < allFQDataValues.length; r++) {
                        List<CString> newRow = new ArrayList<>(dataValuesToProcess.size());
                        for (int c = 0; c < dataValuesToProcess.size(); c++) {
                            if (dataValuesToProcess.get(c)) {
                                String newAttributeName = cspMapping.get(allFQAttributeNames[c]);
                                if (newAttributeName != null && Arrays.stream(newAttributeNames[csp])
                                        .anyMatch(nan -> nan.equals(newAttributeName))) {
                                    newRow.add(allFQDataValues[r][c]);
                                }
                            } else {
                                newRow.add(allFQDataValues[r][c]);
                            }
                        }
                        // add additional data values
                        for (int c = 0; c < newAttributeNamesIndex.size(); c++) {
                            if (newAttributeNamesIndex.get(c) == -1) {
                                newRow.add(null);
                            }
                        }
                        newDataValues.add(newRow);
                    }
                    newOutboundDataOperation.setDataValues(newDataValues);
                    Map<CString, CString> dataIdMapping = cspMapping.entrySet().stream().collect(
                            Collectors.toMap(e -> CString.valueOf(e.getKey()), e -> CString.valueOf(e.getValue())));
                    newOutboundDataOperation.setDataIdMapping(dataIdMapping);
                    List<SimpleEntry<Criteria, Boolean>> newCriteriaTrack = Arrays.stream(newCriteria[csp])
                            .map(nc -> new SimpleEntry<>(nc, false)).collect(Collectors.toList());
                    List<OutboundDataOperation.Criterion> criterions = new ArrayList<>(protectedCriteria.size());
                    for (int i = 0; i < protectedCriteria.size(); i++) {
                        if (protectedCriteria.get(i)) {
                            String clearCriteriaAttributeName = allCriteriaAttributeNames[i];
                            String cspProtectedCriteriaAttributeName = cspMapping.get(clearCriteriaAttributeName);
                            Map.Entry<Criteria, Boolean> entry = null;
                            if (cspProtectedCriteriaAttributeName != null) {
                                entry = newCriteriaTrack.stream().filter(e -> !e.getValue()).findFirst().orElse(null);
                            }
                            if (entry != null) {
                                entry.setValue(true);
                                Criteria cspProtectedCriteria = entry.getKey();
                                criterions.add(new OutboundDataOperation.Criterion(
                                        CString.valueOf(cspProtectedCriteria.getAttributeName()),
                                        CString.valueOf(cspProtectedCriteria.getOperator()),
                                        CString.valueOf(cspProtectedCriteria.getValue())));
                            }
                        } else {
                            criterions.add(allCriterions.get(i));
                        }
                    }
                    newOutboundDataOperation.setCriterions(criterions);
                    newOutboundDataOperation.setPromise(promise);
                    boolean modified = outboundDataOperation.isModified();
                    if (!modified) {
                        modified = numberOfCSPs > 1;
                    }
                    if (!modified) {
                        modified = !cspMapping.entrySet().stream()
                                .allMatch(e -> e.getValue() == null || e.getKey().equals(e.getValue()));
                    }
                    if (!modified) {
                        String[] criteriaOperators = Arrays.stream(criteria).map(Criteria::getOperator)
                                .toArray(String[]::new);
                        String[] protectedCriteriaOperators = Arrays.stream(newCriteria[csp]).map(Criteria::getOperator)
                                .toArray(String[]::new);
                        modified = !Arrays.equals(criteriaOperators, protectedCriteriaOperators);
                    }
                    if (!modified) {
                        String[] criteriaValues = Arrays.stream(criteria).map(Criteria::getValue)
                                .toArray(String[]::new);
                        String[] protectedCriteriaValues = Arrays.stream(newCriteria[csp]).map(Criteria::getValue)
                                .toArray(String[]::new);
                        modified = !Arrays.equals(criteriaValues, protectedCriteriaValues);
                    }
                    newOutboundDataOperation.setModified(modified);
                    newOutboundDataOperation.setInvolvedCSP(csp);
                    newOutboundDataOperations.add(newOutboundDataOperation);
                }
            }
        }
        if (newOutboundDataOperations == null) {
            // nothing to protect
            if (outboundDataOperation.getInvolvedCSPs() != null) {
                // retain only the CSPs involved by the protection module
                outboundDataOperation.getInvolvedCSPs().removeIf(csp -> csp >= numberOfCSPs);
            }
            if (outboundDataOperation.getInvolvedCSPs() == null || !outboundDataOperation.getInvolvedCSPs().isEmpty()) {
                if (outboundDataOperation.getInvolvedCSPs() == null
                        || outboundDataOperation.getInvolvedCSPs().size() == 1 || !outboundDataOperation.isModified()) {
                    Map<CString, CString> dataIdMapping = outboundDataOperation.getDataIds().stream().distinct()
                            .collect(Collectors.toMap(Function.identity(), Function.identity()));
                    outboundDataOperation.setDataIdMapping(dataIdMapping);
                    newOutboundDataOperations = Collections.singletonList(outboundDataOperation);
                } else {
                    // broadcast operation to all involved CSPs
                    newOutboundDataOperations = outboundDataOperation.getInvolvedCSPs().stream().map(csp -> {
                        OutboundDataOperation newOutboundDataOperation = new OutboundDataOperation();
                        newOutboundDataOperation.setAttributes(outboundDataOperation.getAttributes());
                        newOutboundDataOperation.setRequestId(outboundDataOperation.getRequestId());
                        newOutboundDataOperation.setOperation(outboundDataOperation.getOperation());
                        newOutboundDataOperation.setUsingHeadOperation(outboundDataOperation.isUsingHeadOperation());
                        newOutboundDataOperation
                                .setUnprotectingDataEnabled(outboundDataOperation.isUnprotectingDataEnabled());
                        newOutboundDataOperation.setDataIds(new ArrayList<>(outboundDataOperation.getDataIds()));
                        newOutboundDataOperation.setDataValues(new ArrayList<>(outboundDataOperation.getDataValues()));
                        Map<CString, CString> dataIdMapping = newOutboundDataOperation.getDataIds().stream().distinct()
                                .collect(Collectors.toMap(Function.identity(), Function.identity()));
                        newOutboundDataOperation.setDataIdMapping(dataIdMapping);
                        newOutboundDataOperation
                                .setCriterions(outboundDataOperation.getCriterions().stream()
                                        .map(criterion -> new OutboundDataOperation.Criterion(criterion.getDataId(),
                                                criterion.getOperator(), criterion.getValue()))
                                        .collect(Collectors.toList()));
                        newOutboundDataOperation.setPromise(outboundDataOperation.getPromise());
                        newOutboundDataOperation.setModified(true);
                        newOutboundDataOperation.setInvolvedCSP(csp);
                        return newOutboundDataOperation;
                    }).collect(Collectors.toList());
                }
            } else {
                newOutboundDataOperations = Collections.emptyList();
            }
        }
        return newOutboundDataOperations;
    }

    private String escapeRegex(String regex) {
        return regex.replace(".", "\\.").replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)")
                .replace("*", "[^/]*");
    }

    private InboundDataOperation newInboundReadOperation(List<InboundDataOperation> inboundDataOperations) {
        inboundDataOperations.forEach(dataOperation -> {
            if (dataOperation.isUsingHeadOperation()) {
                // using head operation is not supported (because of the
                // promise)
                throw new IllegalArgumentException("Read operation cannot rely only on the head operation");
            }
        });
        InboundDataOperation newInboundDataOperation = null;
        @SuppressWarnings("serial")
        DataOperationCommand defaultDataOperationCommand = new DataOperationCommand() {
            @Override
            public Map<String, String> getMapping() {
                return Collections.emptyMap();
            }
        };
        List<DataOperationCommand> promise = inboundDataOperations.stream().map(DataOperation::getPromise)
                .filter(p -> p != null).findAny().orElseGet(() -> {
                    int numberOfCSPs = inboundDataOperations.stream().map(DataOperation::getInvolvedCSP)
                            .max(Comparator.naturalOrder()).orElse(0);
                    return Stream.<DataOperationCommand>generate(() -> defaultDataOperationCommand).limit(numberOfCSPs)
                            .collect(Collectors.toList());
                });
        List<Map<String, String>> mappings = promise.stream().map(DataOperationCommand::getMapping)
                .collect(Collectors.toList());
        int numberOfCSPs = mappings.size();
        // Filter protected attribute names according to the mapping
        // (in order to pass only protected attributes that have to be
        // unprotected)
        String[] allAttributeNames = inboundDataOperations.stream().map(InboundDataOperation::getClearDataIds).findAny()
                .get().stream().map(StringUtilities::toString).toArray(String[]::new);
        List<String[]> allProtectedAttributeNames = IntStream.range(0, numberOfCSPs)
                .mapToObj(csp -> inboundDataOperations.stream().filter(ido -> ido.getInvolvedCSP() == csp).findFirst()
                        .orElse(null))
                .map(ido -> ido == null ? new String[0]
                        : ido.getDataIds().stream().map(StringUtilities::toString).toArray(String[]::new))
                .collect(Collectors.toList());
        List<List<Boolean>> allProtectedAttributeProtectionFlags = IntStream.range(0, numberOfCSPs)
                .mapToObj(csp -> Arrays.stream(allProtectedAttributeNames.get(csp))
                        .map(pan -> mappings.get(csp).values().contains(pan)).collect(Collectors.toList()))
                .collect(Collectors.toList());
        List<String[]> protectedAttributeNames = IntStream.range(0, numberOfCSPs)
                .mapToObj(csp -> IntStream.range(0, allProtectedAttributeProtectionFlags.get(csp).size())
                        .filter(i -> allProtectedAttributeProtectionFlags.get(csp).get(i))
                        .mapToObj(i -> allProtectedAttributeNames.get(csp)[i]).distinct().toArray(String[]::new))
                .collect(Collectors.toList());
        if (protectedAttributeNames.stream().map(Array::getLength).anyMatch(len -> len > 0)) {
            // at least one attribute to unprotect
            // all the protected content (including content that don't need to
            // be unprotected and content for duplicated attribute names) for
            // all CSPs
            List<String[][]> allProtectedContents = IntStream.range(0, numberOfCSPs)
                    .mapToObj(csp -> inboundDataOperations.stream().filter(ido -> ido.getInvolvedCSP() == csp)
                            .findFirst().orElse(null))
                    .map(ido -> ido == null ? new String[0][]
                            : ido.getDataValues().stream()
                                    .map(row -> row.stream().map(StringUtilities::toString).toArray(String[]::new))
                                    .toArray(String[][]::new))
                    .collect(Collectors.toList());
            // indexes in the protected attribute names expected by the
            // protection module for all the request protected attribute names
            // (-1 for protected attribute names that don't need to be
            // unprotected and for duplicated protected attribute names) and for
            // all CSPs
            List<List<Map.Entry<String, Integer>>> allProtectedAttributeNameIndexes = IntStream.range(0, numberOfCSPs)
                    .mapToObj(csp -> {
                        String[] pans = allProtectedAttributeNames.get(csp);
                        String[] cspProtectedAttributeNames = promise.get(csp).getProtectedAttributeNames().clone();
                        return Arrays.stream(pans).<Map.Entry<String, Integer>>map(pan -> {
                            int index = IntStream.range(0, cspProtectedAttributeNames.length)
                                    .filter(i -> pan.equals(cspProtectedAttributeNames[i])).findFirst().orElse(-1);
                            if (index != -1) {
                                // avoid duplicated protected attribute names
                                cspProtectedAttributeNames[index] = null;
                            }
                            return new SimpleEntry<>(pan, index);
                        }).collect(Collectors.toList());
                    }).collect(Collectors.toList());
            // protected content to pass to the protection module (excluding
            // content that don't need to be unprotected and excluding content
            // for duplicated attribute names and sorted according to the order
            // of the protected attributes defined in the promise) for all CSPs
            List<String[][]> moduleProtectedContents = IntStream.range(0, numberOfCSPs)
                    .mapToObj(
                            csp -> Arrays.stream(allProtectedContents.get(csp))
                                    .map(allRow -> IntStream.range(0, allProtectedAttributeNameIndexes.get(csp).size())
                                            .filter(i -> allProtectedAttributeNameIndexes.get(csp).get(i)
                                                    .getValue() != -1)
                                            .mapToObj(Integer::valueOf)
                                            .sorted((i1, i2) -> allProtectedAttributeNameIndexes.get(csp).get(i1)
                                                    .getValue()
                                                    - allProtectedAttributeNameIndexes.get(csp).get(i2).getValue())
                                            .map(i -> allRow[i]).toArray(String[]::new))
                                    .toArray(String[][]::new))
                    .collect(Collectors.toList());
            List<DataOperationResult> results = get(promise, moduleProtectedContents);
            String[] moduleNewAttributeNames;
            String[][] moduleNewContents;
            if (results == null || (results.size() == 1 && results.get(0) instanceof DataOperationResponse)) {
                moduleNewAttributeNames = results == null ? new String[0]
                        : ((DataOperationResponse) results.get(0)).getAttributeNames();
                moduleNewContents = results == null ? new String[0][]
                        : ((DataOperationResponse) results.get(0)).getContents();
            } else {
                throw new IllegalStateException("not yet supported");
            }
            boolean same = true;
            if (numberOfCSPs != 1) {
                same = false;
            } else {
                String[][] cspModuleProtectedContents = moduleProtectedContents.get(0);
                if (cspModuleProtectedContents != moduleNewContents) {
                    if (cspModuleProtectedContents.length != moduleNewContents.length) {
                        same = false;
                    } else {
                        loop1: for (int i = 0; i < cspModuleProtectedContents.length; i++) {
                            if (cspModuleProtectedContents[i] != moduleNewContents[i]) {
                                if (cspModuleProtectedContents[i].length != moduleNewContents[i].length) {
                                    same = false;
                                    break loop1;
                                } else {
                                    for (int j = 0; j < cspModuleProtectedContents[i].length; j++) {
                                        if (cspModuleProtectedContents[i][j] != moduleNewContents[i][j]) {
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
            if (!same) {
                String[][] newContents = moduleNewContents;
                // indexes in the attribute names for each request attribute
                // name (-1 if an attribute name didn't need to be unprotected)
                List<Map.Entry<String, Integer>> allAttributeNameIndexes = Arrays
                        .stream(allAttributeNames).<Map.Entry<String, Integer>>map(an -> {
                            int index = IntStream.range(0, moduleNewAttributeNames.length)
                                    .filter(i -> an.equals(moduleNewAttributeNames[i])).findFirst().orElse(-1);
                            return new SimpleEntry<>(an, index);
                        }).collect(Collectors.toList());
                // prepare flags to know if content must be duplicated
                boolean[][] newContentFlags = Arrays.stream(newContents).map(row -> new boolean[row.length])
                        .peek(row -> Arrays.fill(row, false)).toArray(boolean[][]::new);
                int[] positions = new int[mappings.size()];
                Arrays.fill(positions, 0);
                // all the unprotected content (including unprotected content
                // returned by the protection module and including content that
                // didn't need to be unprotected)
                String[][] allNewContents = IntStream.range(0, newContents.length).mapToObj(r -> {
                    String[] newRow = new String[allAttributeNames.length];
                    for (int c = 0; c < allAttributeNames.length; c++) {
                        String value;
                        String attributeName = allAttributeNames[c];
                        int idx = allAttributeNameIndexes.get(c).getValue();
                        if (idx != -1) {
                            String v = newContents[r][idx];
                            value = v != null && newContentFlags[r][idx] ? new String(v) : v;
                            newContentFlags[r][idx] = true;
                        } else {
                            int csp = IntStream.range(0, mappings.size())
                                    .filter(i -> mappings.get(i).containsKey(attributeName)).findFirst().orElse(-1);
                            if (csp != -1) {
                                String protectedAttributeName = mappings.get(csp).get(attributeName);
                                List<Entry<String, Integer>> panIndexes = allProtectedAttributeNameIndexes.get(csp);
                                panIndexes = panIndexes.subList(positions[csp], panIndexes.size());
                                idx = panIndexes.stream().filter(e -> e.getKey().equals(protectedAttributeName))
                                        .map(Map.Entry::getValue).mapToInt(Integer::intValue).findFirst().getAsInt();
                                positions[csp] = idx + 1;
                            } else {
                                csp = 0;
                                List<String> pans = Arrays.asList(allProtectedAttributeNames.get(csp));
                                pans = pans.subList(positions[csp], pans.size());
                                idx = pans.indexOf(attributeName);
                                positions[csp] = idx + 1;
                            }
                            value = allProtectedContents.get(csp)[r][idx];
                        }
                        newRow[c] = value;
                    }
                    return newRow;
                }).toArray(String[][]::new);
                // new values (preserving original values that were not modified)
                List<List<CString>> newDataValues = IntStream.range(0, allNewContents.length)
                        .mapToObj(r -> Arrays.stream(allNewContents[r]).map(v -> {
                            CString dataValue = null;
                            boolean found = false;
                            loop1: for (int csp = 0; csp < allProtectedContents.size(); csp++) {
                                String[][] cspContent = allProtectedContents.get(csp);
                                if (r < cspContent.length) {
                                    for (int c = 0; c < cspContent[r].length; c++) {
                                        if (v == cspContent[r][c]) {
                                            for (int i = 0; i < inboundDataOperations.size(); i++) {
                                                InboundDataOperation inboundDataOperation = inboundDataOperations
                                                        .get(i);
                                                if (inboundDataOperation.getInvolvedCSP() == csp) {
                                                    dataValue = inboundDataOperation.getDataValues().get(r).get(c);
                                                    found = true;
                                                    break loop1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (!found) {
                                dataValue = CString.valueOf(v);
                            }
                            return dataValue;
                        }).collect(Collectors.toList())).collect(Collectors.toList());
                newInboundDataOperation = new InboundDataOperation();
                newInboundDataOperation.setAttributes(inboundDataOperations.get(0).getAttributes());
                newInboundDataOperation.setRequestId(inboundDataOperations.get(0).getRequestId());
                newInboundDataOperation.setOperation(inboundDataOperations.get(0).getOperation());
                newInboundDataOperation.setUsingHeadOperation(inboundDataOperations.get(0).isUsingHeadOperation());
                newInboundDataOperation
                        .setUnprotectingDataEnabled(inboundDataOperations.get(0).isUnprotectingDataEnabled());
                newInboundDataOperation.setDataIds(inboundDataOperations.get(0).getClearDataIds());
                newInboundDataOperation.setDataValues(newDataValues);
                List<Map<CString, CString>> dataIdMappings = mappings.stream()
                        .map(map -> map.entrySet().stream().collect(
                                Collectors.toMap(e -> CString.valueOf(e.getKey()), e -> CString.valueOf(e.getValue()))))
                        .collect(Collectors.toList());
                newInboundDataOperation.setDataIdMappings(dataIdMappings);
                newInboundDataOperation.setPromise(promise);
                newInboundDataOperation.setModified(true);
                newInboundDataOperation.setInvolvedCSPs(
                        inboundDataOperations.stream().map(DataOperation::getInvolvedCSPs).filter(csps -> csps != null)
                                .flatMap(List::stream).sorted().collect(Collectors.toList()));
                if (newInboundDataOperation.getInvolvedCSPs().isEmpty()) {
                    newInboundDataOperation.setInvolvedCSPs(null);
                }
            }
        }
        if (newInboundDataOperation == null) {
            // nothing to unprotect
            if (inboundDataOperations.size() == 0) {
                throw new IllegalStateException("strange");
            }
            newInboundDataOperation = new InboundDataOperation();
            newInboundDataOperation.setAttributes(inboundDataOperations.get(0).getAttributes());
            newInboundDataOperation.setRequestId(inboundDataOperations.get(0).getRequestId());
            newInboundDataOperation.setOperation(inboundDataOperations.get(0).getOperation());
            newInboundDataOperation.setUsingHeadOperation(inboundDataOperations.get(0).isUsingHeadOperation());
            newInboundDataOperation
                    .setUnprotectingDataEnabled(inboundDataOperations.get(0).isUnprotectingDataEnabled());
            newInboundDataOperation.setDataIds(inboundDataOperations.get(0).getClearDataIds());
            newInboundDataOperation.setDataValues(inboundDataOperations.get(0).getDataValues());
            List<Map<CString, CString>> dataIdMappings;
            if (mappings.isEmpty()) {
                dataIdMappings = Collections.singletonList(inboundDataOperations.get(0).getDataIds().stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity())));
            } else {
                dataIdMappings = Collections.singletonList(mappings.get(0).entrySet().stream().collect(
                        Collectors.toMap(e -> CString.valueOf(e.getKey()), e -> CString.valueOf(e.getValue()))));
            }
            newInboundDataOperation.setDataIdMappings(dataIdMappings);
            newInboundDataOperation.setPromise(inboundDataOperations.get(0).getPromise());
            newInboundDataOperation.setModified(true);
            newInboundDataOperation.setInvolvedCSPs(inboundDataOperations.stream().map(DataOperation::getInvolvedCSPs)
                    .filter(csps -> csps != null).flatMap(List::stream).sorted().collect(Collectors.toList()));
            if (newInboundDataOperation.getInvolvedCSPs().isEmpty()) {
                newInboundDataOperation.setInvolvedCSPs(null);
            }
        }
        return newInboundDataOperation;
    }

    private List<OutboundDataOperation> newUpdateOperation(OutboundDataOperation outboundDataOperation) {
        List<OutboundDataOperation> newOutboundDataOperations = null;
        // Filter attribute names according to the mapping
        // (in order to pass only attributes that have to be protected)
        String[] allAttributeNames = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .toArray(String[]::new);
        List<Criterion> allCriterions = outboundDataOperation.getCriterions();
        String[] allCriteriaAttributeNames = allCriterions.stream()
                .map(criterion -> criterion != null ? criterion.getDataId().toString() : null).toArray(String[]::new);
        List<Map<String, String>> mappings = head(Stream
                .concat(Stream.of(allAttributeNames), Stream.of(allCriteriaAttributeNames).filter(can -> can != null))
                .toArray(String[]::new));
        int numberOfCSPs = mappings.size();
        List<Boolean> protectedAttributes = Arrays.stream(allAttributeNames)
                .map(an -> mappings.stream().anyMatch(map -> map.containsKey(an))).collect(Collectors.toList());
        String[] attributeNames = IntStream.range(0, allAttributeNames.length).filter(i -> protectedAttributes.get(i))
                .mapToObj(i -> allAttributeNames[i]).toArray(String[]::new);
        Criteria[] allCriteria = allCriterions.stream()
                .map(criterion -> criterion != null ? new Criteria(criterion.getDataId().toString(),
                        criterion.getOperator().toString(), criterion.getValue().toString()) : null)
                .toArray(Criteria[]::new);
        List<Boolean> protectedCriteria = Arrays.stream(allCriteriaAttributeNames)
                .map(can -> can != null && mappings.stream().anyMatch(map -> map.containsKey(can)))
                .collect(Collectors.toList());
        Criteria[] criteria = IntStream.range(0, allCriteria.length).filter(i -> protectedCriteria.get(i))
                .mapToObj(i -> allCriteria[i]).toArray(Criteria[]::new);
        int[] criteriaMapping = IntStream.range(0, allCriteria.length)
                .map(i -> protectedCriteria.get(i) ? Arrays.asList(criteria).indexOf(allCriteria[i]) : -1).toArray();
        if (attributeNames.length > 0 || criteria.length > 0) {
            // at least one attribute has to be protected
            String[][] allContents;
            String[][] contents;
            String[][] newAttributeNames;
            String[][][] newContents;
            List<Map<String, Integer>> mappingIndexes;
            Criteria[][] newCriteria;
            if (outboundDataOperation.isUsingHeadOperation()) {
                // using head operation is more efficient when there is no
                // content to protect and no criteria
                if (!outboundDataOperation.getDataValues().isEmpty()) {
                    throw new IllegalStateException("cannot use head operation to protect content");
                }
                if (allCriteria.length > 0) {
                    throw new IllegalStateException("cannot use head operation with criteria");
                }
                allContents = new String[0][];
                contents = new String[0][];
                newAttributeNames = Stream.<String[]>generate(() -> attributeNames).limit(numberOfCSPs)
                        .toArray(String[][]::new);
                newContents = Stream.<String[][]>generate(() -> contents).limit(numberOfCSPs)
                        .toArray(String[][][]::new);
                mappingIndexes = Stream.<Map<String, Integer>>generate(() -> Collections.emptyMap()).limit(numberOfCSPs)
                        .collect(Collectors.toList());
                newCriteria = Stream.<Criteria[]>generate(() -> criteria).limit(numberOfCSPs)
                        .toArray(Criteria[][]::new);
            } else {
                // using put operation allow to protect content
                allContents = outboundDataOperation.getDataValues().stream()
                        .map(row -> row.stream().map(StringUtilities::toString).toArray(String[]::new))
                        .toArray(String[][]::new);
                contents = Arrays.stream(allContents)
                        .map(allRow -> IntStream.range(0, protectedAttributes.size())
                                .filter(i -> protectedAttributes.get(i)).mapToObj(i -> allRow[i])
                                .toArray(String[]::new))
                        .toArray(String[][]::new);
                List<DataOperationCommand> results = put(attributeNames, criteria, contents);
                newAttributeNames = results == null
                        ? Stream.<String[]>generate(() -> attributeNames).limit(numberOfCSPs).toArray(String[][]::new)
                        : results.stream().map(DataOperationCommand::getProtectedAttributeNames)
                                .map(pans -> pans == null ? new String[0] : pans).toArray(String[][]::new);
                newContents = results == null
                        ? Stream.<String[][]>generate(() -> contents).limit(numberOfCSPs).toArray(String[][][]::new)
                        : results.stream().map(DataOperationCommand::getProtectedContents).toArray(String[][][]::new);
                mappingIndexes = results == null
                        ? Stream.<Map<String, Integer>>generate(() -> Collections.emptyMap()).limit(numberOfCSPs)
                                .collect(Collectors.toList())
                        : results.stream()
                                .map(result -> result.getMapping().entrySet().stream()
                                        .collect(Collectors.toMap(Map.Entry::getKey,
                                                e -> IntStream.range(0, result.getProtectedAttributeNames().length)
                                                        .filter(i -> result.getProtectedAttributeNames()[i]
                                                                .equals(e.getValue()))
                                                        .findFirst().getAsInt())))
                                .collect(Collectors.toList());
                newCriteria = results == null
                        ? Stream.<Criteria[]>generate(() -> criteria).limit(numberOfCSPs).toArray(Criteria[][]::new)
                        : results.stream().map(DataOperationCommand::getCriteria)
                                .map(cs -> cs == null ? new Criteria[0] : cs).toArray(Criteria[][]::new);
            }
            if (numberOfCSPs > 1 // more than one csp
                    // one csp but attribute names change
                    || (numberOfCSPs == 1 && !mappings.get(0).entrySet().stream()
                            .allMatch(e -> e.getValue() == null || e.getKey().equals(e.getValue())))
                    // one csp but content change
                    || (newContents.length == 1 && !Arrays.equals(contents, newContents[0]))
                    // one csp but criteria change
                    || (newCriteria.length == 1 && !Arrays.equals(criteria, newCriteria[0]))) {
                newOutboundDataOperations = new ArrayList<>(numberOfCSPs);
                for (int csp = 0; csp < numberOfCSPs; csp++) {
                    if (outboundDataOperation.getInvolvedCSPs() != null
                            && !outboundDataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    Map<String, String> cspMapping = mappings.get(csp);
                    if (cspMapping.size() == 0 || newAttributeNames[csp].length == 0) {
                        continue;
                    }
                    Map<String, Integer> cspMappingIndexes = mappingIndexes.get(csp);
                    OutboundDataOperation newOutboundDataOperation = new OutboundDataOperation();
                    newOutboundDataOperation.setAttributes(outboundDataOperation.getAttributes());
                    newOutboundDataOperation.setRequestId(outboundDataOperation.getRequestId());
                    newOutboundDataOperation.setOperation(outboundDataOperation.getOperation());
                    newOutboundDataOperation.setUsingHeadOperation(outboundDataOperation.isUsingHeadOperation());
                    newOutboundDataOperation
                            .setUnprotectingDataEnabled(outboundDataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = new ArrayList<>(protectedAttributes.size());
                    for (int c = 0; c < protectedAttributes.size(); c++) {
                        if (protectedAttributes.get(c)) {
                            String newAttributeName = cspMapping.get(allAttributeNames[c]);
                            if (newAttributeName != null && Arrays.stream(newAttributeNames[csp])
                                    .anyMatch(nan -> nan.equals(newAttributeName))) {
                                dataIds.add(CString.valueOf(newAttributeName));
                            }
                        } else {
                            dataIds.add(CString.valueOf(allAttributeNames[c]));
                        }
                    }
                    newOutboundDataOperation.setDataIds(dataIds);
                    String[][] cspNewContents = newContents[csp];
                    List<List<CString>> newDataValues = new ArrayList<>(allContents.length);
                    for (int r = 0; r < allContents.length; r++) {
                        List<CString> rowDataValues = new ArrayList<>(protectedAttributes.size());
                        for (int c = 0; c < protectedAttributes.size(); c++) {
                            if (protectedAttributes.get(c)) {
                                String newAttributeName = cspMapping.get(allAttributeNames[c]);
                                if (newAttributeName != null && Arrays.stream(newAttributeNames[csp])
                                        .anyMatch(nan -> nan.equals(newAttributeName))) {
                                    rowDataValues.add(CString
                                            .valueOf(cspNewContents[r][cspMappingIndexes.get(allAttributeNames[c])]));
                                }
                            } else {
                                rowDataValues.add(CString.valueOf(allContents[r][c]));
                            }
                        }
                        newDataValues.add(rowDataValues);
                    }
                    newOutboundDataOperation.setDataValues(newDataValues);
                    Map<CString, CString> dataIdMapping = cspMapping.entrySet().stream().collect(
                            Collectors.toMap(e -> CString.valueOf(e.getKey()), e -> CString.valueOf(e.getValue())));
                    newOutboundDataOperation.setDataIdMapping(dataIdMapping);
                    List<OutboundDataOperation.Criterion> criterions = new ArrayList<>(protectedCriteria.size());
                    for (int i = 0; i < protectedCriteria.size(); i++) {
                        if (protectedCriteria.get(i)) {
                            int idx = criteriaMapping[i];
                            if (idx != -1) {
                                criterions.add(new OutboundDataOperation.Criterion(
                                        CString.valueOf(newCriteria[csp][idx].getAttributeName()),
                                        CString.valueOf(newCriteria[csp][idx].getOperator()),
                                        CString.valueOf(newCriteria[csp][idx].getValue())));
                            }
                        } else {
                            criterions.add(allCriterions.get(i));
                        }
                    }
                    newOutboundDataOperation.setCriterions(criterions);
                    newOutboundDataOperation.setModified(true);
                    newOutboundDataOperation.setInvolvedCSP(csp);
                    newOutboundDataOperations.add(newOutboundDataOperation);
                }
            }
        }
        if (newOutboundDataOperations == null) {
            // nothing to protect
            if (outboundDataOperation.getInvolvedCSPs() != null) {
                // retain only the first CSP
                outboundDataOperation.getInvolvedCSPs().removeIf(csp -> csp != 0);
            }
            if (outboundDataOperation.getInvolvedCSPs() == null || !outboundDataOperation.getInvolvedCSPs().isEmpty()) {
                outboundDataOperation.setInvolvedCSP(0);
                Map<CString, CString> dataIdMapping = outboundDataOperation.getDataIds().stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
                outboundDataOperation.setDataIdMapping(dataIdMapping);
                newOutboundDataOperations = Collections.singletonList(outboundDataOperation);
            } else {
                newOutboundDataOperations = Collections.emptyList();
            }
        }
        return newOutboundDataOperations;
    }

    private List<OutboundDataOperation> newDeleteOperation(OutboundDataOperation outboundDataOperation) {
        List<OutboundDataOperation> newOutboundDataOperations = null;
        // Filter attribute names according to the mapping
        // (in order to pass only attributes that have to be deleted)
        String[] allAttributeNames = outboundDataOperation.getDataIds().stream().map(CString::toString)
                .toArray(String[]::new);
        List<Criterion> allCriterions = outboundDataOperation.getCriterions();
        String[] allCriteriaAttributeNames = allCriterions.stream()
                .map(criterion -> criterion != null ? criterion.getDataId().toString() : null).toArray(String[]::new);
        List<Map<String, String>> mappings = head(Stream
                .concat(Stream.of(allAttributeNames), Stream.of(allCriteriaAttributeNames).filter(can -> can != null))
                .toArray(String[]::new));
        int numberOfCSPs = mappings.size();
        List<String> fqAttributeNames = mappings.stream().map(Map::keySet).flatMap(Set::stream).distinct()
                .collect(Collectors.toList());
        String[] allFQAttributeNames = Arrays.stream(allAttributeNames).flatMap(attributeName -> {
            Stream<String> stream = Stream.of(attributeName);
            if (attributeName.indexOf('*') != -1) {
                Pattern pattern = Pattern.compile(escapeRegex(attributeName));
                if (fqAttributeNames.stream().anyMatch(fqan -> pattern.matcher(fqan).matches())) {
                    stream = fqAttributeNames.stream().filter(fqan -> pattern.matcher(fqan).matches());
                }
            }
            return stream;
        }).toArray(String[]::new);
        List<Boolean> protectedAttributes = Arrays.stream(allFQAttributeNames)
                .map(an -> mappings.stream().anyMatch(map -> map.containsKey(an))).collect(Collectors.toList());
        String[] attributeNames = IntStream.range(0, allFQAttributeNames.length).filter(i -> protectedAttributes.get(i))
                .mapToObj(i -> allFQAttributeNames[i]).distinct().toArray(String[]::new);
        Criteria[] allCriteria = allCriterions.stream()
                .map(criterion -> criterion != null ? new Criteria(criterion.getDataId().toString(),
                        criterion.getOperator().toString(), criterion.getValue().toString()) : null)
                .toArray(Criteria[]::new);
        List<Boolean> protectedCriteria = Arrays.stream(allCriteriaAttributeNames)
                .map(can -> can != null && mappings.stream().anyMatch(map -> map.containsKey(can)))
                .collect(Collectors.toList());
        Criteria[] criteria = IntStream.range(0, allCriteria.length).filter(i -> protectedCriteria.get(i))
                .mapToObj(i -> allCriteria[i]).toArray(Criteria[]::new);
        int[] criteriaMapping = IntStream.range(0, allCriteria.length)
                .map(i -> protectedCriteria.get(i) ? Arrays.asList(criteria).indexOf(allCriteria[i]) : -1).toArray();
        if (attributeNames.length > 0 || criteria.length > 0) {
            // at least one attribute has to be deleted
            String[][] newAttributeNames;
            Criteria[][] newCriteria;
            if (outboundDataOperation.isUsingHeadOperation()) {
                // using head operation is more efficient when there is no
                // criteria
                if (allCriteria.length > 0) {
                    throw new IllegalStateException("cannot use head operation with criteria");
                }
                newAttributeNames = Stream.<String[]>generate(() -> attributeNames).limit(numberOfCSPs)
                        .toArray(String[][]::new);
                newCriteria = Stream.<Criteria[]>generate(() -> criteria).limit(numberOfCSPs)
                        .toArray(Criteria[][]::new);
            } else {
                // using delete operation allow to delete content
                List<DataOperationCommand> results = delete(attributeNames, criteria);
                newAttributeNames = results == null
                        ? Stream.<String[]>generate(() -> attributeNames).limit(numberOfCSPs).toArray(String[][]::new)
                        : results.stream().map(DataOperationCommand::getProtectedAttributeNames)
                                .map(pans -> pans == null ? new String[0] : pans).toArray(String[][]::new);
                newCriteria = results == null ? new Criteria[numberOfCSPs][0]
                        : results.stream().map(DataOperationCommand::getCriteria)
                                .map(cs -> cs == null ? new Criteria[0] : cs).toArray(Criteria[][]::new);
            }
            if (numberOfCSPs > 1 // more than one csp
                    // one csp but attribute names change
                    || (numberOfCSPs == 1 && !mappings.get(0).entrySet().stream()
                            .allMatch(e -> e.getValue() == null || e.getKey().equals(e.getValue())))
                    // one csp but criteria change
                    || (newCriteria.length == 1 && !Arrays.equals(criteria, newCriteria[0]))) {
                newOutboundDataOperations = new ArrayList<>(numberOfCSPs);
                for (int csp = 0; csp < numberOfCSPs; csp++) {
                    if (outboundDataOperation.getInvolvedCSPs() != null
                            && !outboundDataOperation.getInvolvedCSPs().contains(csp)) {
                        continue;
                    }
                    Map<String, String> cspMapping = mappings.get(csp);
                    if (cspMapping.size() == 0 || newAttributeNames[csp].length == 0) {
                        continue;
                    }
                    OutboundDataOperation newOutboundDataOperation = new OutboundDataOperation();
                    newOutboundDataOperation.setAttributes(outboundDataOperation.getAttributes());
                    newOutboundDataOperation.setRequestId(outboundDataOperation.getRequestId());
                    newOutboundDataOperation.setOperation(outboundDataOperation.getOperation());
                    newOutboundDataOperation.setUsingHeadOperation(outboundDataOperation.isUsingHeadOperation());
                    newOutboundDataOperation
                            .setUnprotectingDataEnabled(outboundDataOperation.isUnprotectingDataEnabled());
                    List<CString> dataIds = new ArrayList<>(protectedAttributes.size());
                    for (int c = 0; c < protectedAttributes.size(); c++) {
                        if (protectedAttributes.get(c)) {
                            String newAttributeName = cspMapping.get(allFQAttributeNames[c]);
                            if (newAttributeName != null && Arrays.stream(newAttributeNames[csp])
                                    .anyMatch(nan -> nan.equals(newAttributeName))) {
                                dataIds.add(CString.valueOf(newAttributeName));
                            }
                        } else {
                            dataIds.add(CString.valueOf(allFQAttributeNames[c]));
                        }
                    }
                    newOutboundDataOperation.setDataIds(dataIds);
                    Map<CString, CString> dataIdMapping = cspMapping.entrySet().stream().collect(
                            Collectors.toMap(e -> CString.valueOf(e.getKey()), e -> CString.valueOf(e.getValue())));
                    newOutboundDataOperation.setDataIdMapping(dataIdMapping);
                    List<OutboundDataOperation.Criterion> criterions = new ArrayList<>(protectedCriteria.size());
                    for (int i = 0; i < protectedCriteria.size(); i++) {
                        if (protectedCriteria.get(i)) {
                            int idx = criteriaMapping[i];
                            if (idx != -1) {
                                criterions.add(new OutboundDataOperation.Criterion(
                                        CString.valueOf(newCriteria[csp][idx].getAttributeName()),
                                        CString.valueOf(newCriteria[csp][idx].getOperator()),
                                        CString.valueOf(newCriteria[csp][idx].getValue())));
                            }
                        } else {
                            criterions.add(allCriterions.get(i));
                        }
                    }
                    newOutboundDataOperation.setCriterions(criterions);
                    newOutboundDataOperation.setModified(true);
                    newOutboundDataOperation.setInvolvedCSP(csp);
                    newOutboundDataOperations.add(newOutboundDataOperation);
                }
            }
        }
        if (newOutboundDataOperations == null) {
            // nothing to protect
            if (outboundDataOperation.getInvolvedCSPs() != null) {
                // retain only the first CSP
                outboundDataOperation.getInvolvedCSPs().removeIf(csp -> csp != 0);
            }
            if (outboundDataOperation.getInvolvedCSPs() == null || !outboundDataOperation.getInvolvedCSPs().isEmpty()) {
                outboundDataOperation.setInvolvedCSP(0);
                Map<CString, CString> dataIdMapping = outboundDataOperation.getDataIds().stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
                outboundDataOperation.setDataIdMapping(dataIdMapping);
                newOutboundDataOperations = Collections.singletonList(outboundDataOperation);
            } else {
                newOutboundDataOperations = Collections.emptyList();
            }
        }
        return newOutboundDataOperations;
    }

    private List<OutboundDataOperation> broadcastOutboundOperation(OutboundDataOperation outboundDataOperation) {
        List<OutboundDataOperation> newOutboundDataOperations;
        if (outboundDataOperation.getInvolvedCSPs() != null && outboundDataOperation.getInvolvedCSPs().size() > 1) {
            newOutboundDataOperations = outboundDataOperation.getInvolvedCSPs().stream().map(csp -> {
                OutboundDataOperation newOutboundDataOperation = new OutboundDataOperation();
                newOutboundDataOperation.setModified(true);
                newOutboundDataOperation.setAttributes(outboundDataOperation.getAttributes());
                newOutboundDataOperation.setInvolvedCSP(csp);
                newOutboundDataOperation.setRequestId(outboundDataOperation.getRequestId());
                newOutboundDataOperation.setOperation(outboundDataOperation.getOperation());
                newOutboundDataOperation.setUsingHeadOperation(outboundDataOperation.isUsingHeadOperation());
                newOutboundDataOperation.setDataIds(new ArrayList<>(outboundDataOperation.getDataIds()));
                newOutboundDataOperation.setDataValues(new ArrayList<>(outboundDataOperation.getDataValues()));
                Map<CString, CString> dataIdMapping = outboundDataOperation.getDataIds().stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
                newOutboundDataOperation.setDataIdMapping(dataIdMapping);
                newOutboundDataOperation
                        .setCriterions(
                                outboundDataOperation.getCriterions().stream()
                                        .map(criterion -> new OutboundDataOperation.Criterion(criterion.getDataId(),
                                                criterion.getOperator(), criterion.getValue()))
                                        .collect(Collectors.toList()));
                newOutboundDataOperation.setPromise(outboundDataOperation.getPromise());
                newOutboundDataOperation.setUnprotectingDataEnabled(outboundDataOperation.isUnprotectingDataEnabled());
                return newOutboundDataOperation;
            }).collect(Collectors.toList());
        } else {
            newOutboundDataOperations = Collections.singletonList(outboundDataOperation);
        }
        return newOutboundDataOperations;
    }

    private List<Map<String, String>> head(String[] attributeNames) {
        return protectionModule.getDataOperation().head(attributeNames);
    }

    private List<DataOperationCommand> get(String[] attributeNames, Criteria[] criteria) {
        return protectionModule.getDataOperation().get(attributeNames, criteria);
    }

    private List<DataOperationResult> get(List<DataOperationCommand> promise, List<String[][]> protectedContents) {
        return protectionModule.getDataOperation().get(promise, protectedContents);
    }

    private List<DataOperationCommand> post(String[] attributeNames, String[][] contents) {
        return protectionModule.getDataOperation().post(attributeNames, contents);
    }

    private List<DataOperationCommand> put(String[] attributeNames, Criteria[] criteria, String[][] contents) {
        return protectionModule.getDataOperation().put(attributeNames, criteria, contents);
    }

    private List<DataOperationCommand> delete(String[] attributeNames, Criteria[] criteria) {
        return protectionModule.getDataOperation().delete(attributeNames, criteria);
    }

}
