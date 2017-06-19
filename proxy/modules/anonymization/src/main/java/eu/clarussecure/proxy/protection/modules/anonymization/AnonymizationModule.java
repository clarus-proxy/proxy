package eu.clarussecure.proxy.protection.modules.anonymization;

import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.Criteria;
import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.DataOperationCommand;
import eu.clarussecure.dataoperations.DataOperationResult;
import eu.clarussecure.dataoperations.anonymization.AnonymizeModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class AnonymizationModule implements ProtectionModule, DataOperation {

    private static final String PROTECTION_MODULE_NAME = "Anonymization";
//    private static final String CSP = "csp";
//    private static final Pattern START_WITH_DOUBLE_ASTERISKS = Pattern.compile("^([^/*]*\\*/[^/*]*\\*/)([^/*]*)");
//    private static final Pattern START_WITH_SINGLE_ASTERISK = Pattern.compile("^([^/*]*\\*/)([^/*]*/[^/*]*)");

    private AnonymizeModule anonymizeModule;
//    private String[] dataIds;
//    private Pattern[] fqDataIdPatterns;
//    private String[] datasetPrefixByServer;

    private static class CapabilitiesHelper {
        private static final AnonymizationCapabilities INSTANCE = new AnonymizationCapabilities();
    }

    @Override
    public ProtectionModuleCapabilities getCapabilities() {
        return CapabilitiesHelper.INSTANCE;
    }

    @Override
    public String getProtectionModuleName() {
        return PROTECTION_MODULE_NAME;
    }

    @Override
    public void initialize(Document document, String[] dataIds, String[] datasetPrefixByServer) {
        anonymizeModule = new AnonymizeModule(document);
//        if (dataIds != null) {
//            // Replace unqualified data ids by a generic qualified id (with
//            // asterisks): an1 -> */*/an1, d/an2 -> */d/an2, ds/d/an3 ->
//            // ds/d/an3
//            dataIds = Arrays.stream(dataIds)
//                    .map(id -> id.indexOf('/') == -1
//                            // prepend with */*/ if there is no /
//                            ? "*/*/" + id
//                            : id.indexOf('/') == id.lastIndexOf('/')
//                                    // prepend with */ if there is one /
//                                    ? "*/" + id
//                                    // do nothing if there is two /
//                                    : id)
//                    .toArray(String[]::new);
//            this.dataIds = dataIds;
//            this.fqDataIdPatterns = Arrays.stream(dataIds).map(s -> escapeRegex(s)).map(Pattern::compile)
//                    .toArray(Pattern[]::new);
//            this.datasetPrefixByServer = datasetPrefixByServer;
//        }
    }

    @Override
    public DataOperation getDataOperation() {
        return this;
    }

//    private String escapeRegex(String regex) {
//        return regex.replace(".", "\\.").replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)")
//                .replace("*", "[^/]*");
//    }
//
//    private List<String> resolveAttributes(String[] fqAttributeNames) {
//        List<String> attributeNames;
//        if (Arrays.stream(fqAttributeNames).filter(an -> an.indexOf('*') != -1).count() == 0) {
//            // Attribute names don't contain asterisk (*)
//            attributeNames = Arrays.stream(fqAttributeNames).collect(Collectors.toList());
//        } else {
//            // Replace attribute names that contain asterisk (*) by the matching
//            // data identifiers
//            List<Map.Entry<String, Pattern>> attributeNamePatterns = Arrays.stream(fqAttributeNames)
//                    .map(an -> new SimpleEntry<>(an, Pattern.compile(escapeRegex(an)))).collect(Collectors.toList());
//            Stream<String> retainedDataIds = Arrays.stream(dataIds).filter(id -> id.indexOf('*') == -1);
//            Stream<String> missingDataIds1 = Arrays.stream(dataIds).map(id -> START_WITH_DOUBLE_ASTERISKS.matcher(id))
//                    .filter(m -> m.matches()).map(m -> new String[] { m.group(1), m.group(2) }).flatMap(groups -> {
//                        Pattern firstPartPattern = Pattern.compile(escapeRegex(groups[0]));
//                        String lastPart = groups[1];
//                        return Arrays.stream(fqAttributeNames).map(an -> an.substring(0, an.lastIndexOf('/') + 1))
//                                .filter(an -> firstPartPattern.matcher(an).matches()).map(an -> an + lastPart);
//                    });
//            Stream<String> missingDataIds2 = Arrays.stream(dataIds).map(id -> START_WITH_SINGLE_ASTERISK.matcher(id))
//                    .filter(m -> m.matches()).map(m -> new String[] { m.group(1), m.group(2) }).flatMap(groups -> {
//                        Pattern firstPartPattern = Pattern.compile(escapeRegex(groups[0]));
//                        String lastPart = groups[1];
//                        return Arrays.stream(fqAttributeNames).map(an -> an.substring(0, an.indexOf('/') + 1))
//                                .filter(an -> firstPartPattern.matcher(an).matches()).map(an -> an + lastPart);
//                    });
//            List<String> dataIds = Stream.concat(retainedDataIds, Stream.concat(missingDataIds1, missingDataIds2))
//                    .distinct().collect(Collectors.toList());
//            List<Map.Entry<String, Stream<String>>> resolvedDataIds = attributeNamePatterns.stream()
//                    .map(e -> new SimpleEntry<>(e.getKey(),
//                            dataIds.stream().filter(id -> e.getValue().matcher(id).matches())))
//                    .collect(Collectors.toList());
//            List<Map.Entry<String, Stream<String>>> unresolvedAttributeNames = attributeNamePatterns.stream()
//                    .filter(e -> dataIds.stream().noneMatch(id -> e.getValue().matcher(id).matches()))
//                    .map(e -> new SimpleEntry<>(e.getKey(), Stream.of(e.getKey()))).collect(Collectors.toList());
//            // Concatenate all found attributes
//            List<Map.Entry<String, Stream<String>>> resolvedAttributeNames = Stream
//                    .concat(resolvedDataIds.stream(), unresolvedAttributeNames.stream()).collect(Collectors.toList());
//            attributeNames = resolvedAttributeNames.stream().flatMap(Map.Entry::getValue).collect(Collectors.toList());
//        }
//        return attributeNames;
//    }
//
//    private String buildProtectedAttributeName(int csp, String attributeName) {
//        if (attributeName.chars().filter(c -> c == '/').count() == 2) {
//            if (datasetPrefixByServer != null && csp < datasetPrefixByServer.length) {
//                attributeName = datasetPrefixByServer[csp] + attributeName.substring(attributeName.indexOf('/'));
//            }
//        }
//        attributeName = CSP + (csp + 1) + "/" + attributeName;
//        return attributeName;
//    }
//
//    private List<Map.Entry<String, String>> resolveProtectedAttributes(List<String> attributes) {
//        List<Map.Entry<String, String>> mapping = attributes.stream().map(attr -> {
//            String protectedAttributeName = Arrays.stream(fqDataIdPatterns).anyMatch(p -> p.matcher(attr).matches())
//                    /*|| attr.startsWith("AddGeometryColumn")*/ ? buildProtectedAttributeName(0, attr) : null;
//            return new SimpleEntry<>(attr, protectedAttributeName);
//        }).collect(Collectors.toList());
//        return mapping;
//    }
//
    @Override
    public List<Map<String, String>> head(String[] fqAttributeNames) {
        return anonymizeModule.head(fqAttributeNames);
//        // Assume attribute names are fully qualified (ds/d/a)
//        List<String> attributeNames = resolveAttributes(fqAttributeNames);
//        List<Map.Entry<String, String>> protectedAttributeNames = resolveProtectedAttributes(attributeNames);
//        String[][] mapping = protectedAttributeNames.stream()
//                .map(e -> Stream.concat(Stream.of(e.getKey()), Stream.of(e.getValue())).toArray(String[]::new))
//                .toArray(String[][]::new);
//        return mapping;
    }

    @Override
    public List<DataOperationCommand> get(String[] attributeNames, Criteria[] criteria/* TODO, boolean dispatch*/) {
        return anonymizeModule.get(attributeNames, criteria);
//        // By default, protected attribute names are the same as clear attribute names
//        String[][] protectedAttributeNames = new String[][] { attributeNames };
//        // By default, protected criteria are the same as clear criteria
//        String[][] protectedCriteria = new String[][] { criteria };
//        // TODO workaround (promise not yet implemented in module)
//        //Promise promise = anonymizeModule.get(attributeNames, criteria, operation, dispatch);
//        DefaultPromise promise = new DefaultPromise();
//        promise.setAttributeNames(attributeNames);
//        promise.setCriteria(criteria);
//        promise.setProtectedAttributeNames(protectedAttributeNames);
//        promise.setProtectedCriteria(protectedCriteria);
//        int[][][] attributeMapping = IntStream.range(0, attributeNames.length).mapToObj(i -> new int[][] { { 0, i } })
//                .toArray(int[][][]::new);
//        promise.setAttributeMapping(attributeMapping);
//        return promise;

    }

    @Override
    public List<DataOperationResult> get(List<DataOperationCommand> promise, List<String[][]> contents) {
        return anonymizeModule.get(promise, contents);
//        return contents;
    }

    @Override
    public List<DataOperationCommand> post(String[] attributeNames, String[][] contents) {
        return anonymizeModule.post(attributeNames, contents);
//        String[][][] newContents = anonymizeModule.post(attributeNames, contents);
//        // add protected attributes names as first row
//        newContents = new String[][][] {
//                Stream.concat(Stream.of(new String[][] { attributeNames }), Arrays.stream(newContents[0]))
//                        .toArray(String[][]::new) };
//        return newContents;
    }

    @Override
    public List<DataOperationCommand> put(String[] attributeNames, Criteria[] criteria, String[][] contents) {
        return anonymizeModule.put(attributeNames, criteria, contents);
//        String[][][] newContents = anonymizeModule.put(attributeNames, criteria, contents);
//        // add protected attributes names as first row
//        newContents = new String[][][] {
//                Stream.concat(Stream.of(new String[][] { attributeNames }), Arrays.stream(newContents[0]))
//                        .toArray(String[][]::new) };
//        return newContents;
    }

    @Override
    public List<DataOperationCommand> delete(String[] attributeNames, Criteria[] criteria) {
        return anonymizeModule.delete(attributeNames, criteria);
    }
}
