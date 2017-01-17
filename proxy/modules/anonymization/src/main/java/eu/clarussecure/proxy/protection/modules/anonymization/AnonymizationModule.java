package eu.clarussecure.proxy.protection.modules.anonymization;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.w3c.dom.Document;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.Operation;
import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.dataoperations.anonymization.AnonymizeModule;
import eu.clarussecure.proxy.spi.protection.DefaultPromise;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class AnonymizationModule implements ProtectionModule, DataOperation {

    private static final String PROTECTION_MODULE_NAME = "Anonymization";
    private AnonymizeModule anonymizeModule;
    private String[] dataIds;
    private Pattern[] dataIdPatterns;
    private Pattern[] fqDataIdPatterns;

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
    public void initialize(Document document, String[] dataIds) {
        anonymizeModule = new AnonymizeModule(document);
        if (dataIds != null) {
            // Replace unqualified data ids by a generic qualified id (with asterisks): an1 -> */*/an1, d/an2 -> */d/an2, ds/d/an3 -> ds/d/an3
            dataIds = Arrays.stream(dataIds)
                    .map(id -> id.indexOf('/') == -1 ? "*/*/" + id // prepend with */*/ if there is no /
                            : id.indexOf('/') == id.lastIndexOf('/') ? "*/" + id // prepend with */ if there is one /
                                    : id) // do nothing if there is two /
                    .toArray(String[]::new);
            this.dataIds = dataIds;
            this.dataIdPatterns = Arrays.stream(dataIds).map(s -> s.replace(".", "\\.").replace("*/", "([^/]*/)?"))
                    .map(Pattern::compile).toArray(Pattern[]::new);
            this.fqDataIdPatterns = Arrays.stream(dataIds).map(s -> s.replace(".", "\\.").replace("*", "[^/]*"))
                    .map(Pattern::compile).toArray(Pattern[]::new);
        }
    }

    @Override
    public DataOperation getDataOperation() {
        return this;
    }

    @Override
    public String[][] head(String[] attributeNames) {
        // TODO workaround (not yet implemented in module)
        //return anonymizeModule.head(attributeNames);
        // Replace unqualified attribute names by a generic qualified name (with asterisks): an1 -> */*/an1, d/an2 -> */d/an2, ds/d/an3 -> ds/d/an3
        String[] fqAttributeNames = Arrays.stream(attributeNames)
                .map(an -> an.indexOf('/') == -1 ? "*/*/" + an // prepend with */*/ if there is no /
                        : an.indexOf('/') == an.lastIndexOf('/') ? "*/" + an // prepend with */ if there is one /
                                : an) // do nothing if there is two /
                .toArray(String[]::new);
        // Retain attribute names that don't contain asterisk (*)
        Stream<String> retainedAttributeNames = Arrays.stream(fqAttributeNames)
                .filter(an -> an.indexOf('*') == -1 && an.indexOf('?') == -1);
        // Replace attribute names that contain asterisk (*) by the matching data identifiers
        Map<String, Pattern> attributeNamePatterns = Arrays.stream(fqAttributeNames).filter(an -> an.indexOf('*') != -1)
                .collect(Collectors.toMap(Function.identity(),
                        an -> Pattern.compile(an.replace(".", "\\.").replace("*", "[^/]*"))));
        Stream<String> retainedDataIds = Arrays.stream(dataIds).filter(id -> id.indexOf('*') == -1);
        Stream<String> missingDataIds1 = Arrays.stream(dataIds).filter(id -> id.startsWith("*/*/"))
                .flatMap(id -> Arrays.stream(fqAttributeNames)
                        .map(an -> an.substring(0, an.lastIndexOf('/') + 1) + id.substring("*/*/".length())));
        Stream<String> missingDataIds2 = Arrays.stream(dataIds)
                .filter(id -> id.startsWith("*/") && !id.startsWith("*/*/"))
                .flatMap(id -> Arrays.stream(fqAttributeNames)
                        .map(an -> an.substring(0, an.indexOf('/') + 1) + id.substring("*/".length())));
        List<String> dataIds = Stream.concat(retainedDataIds, Stream.concat(missingDataIds1, missingDataIds2))
                .distinct().collect(Collectors.toList());
        Stream<String> resolvedAttributeNames = dataIds.stream()
                .filter(id -> attributeNamePatterns.values().stream().anyMatch(p -> p.matcher(id).matches()));
        Stream<String> unresolvedAttributeNames = attributeNamePatterns.entrySet().stream()
                .filter(e -> dataIds.stream().noneMatch(id -> e.getValue().matcher(id).matches()))
                .map(Map.Entry::getKey);
        // Concatenate all found attributes
        List<String> attributes = Stream
                .concat(retainedAttributeNames, Stream.concat(resolvedAttributeNames, unresolvedAttributeNames))
                .sorted().collect(Collectors.toList());
        // Remove redundant attributes
        Map<String, Pattern> attributePatterns = attributes.stream().collect(Collectors.toMap(Function.identity(),
                a -> Pattern.compile(a.replace(".", "\\.").replace("*", "[^/]*"))));
        attributes = attributes.stream().filter(a -> attributePatterns.entrySet().stream().filter(e -> a != e.getKey())
                .noneMatch(e -> e.getValue().matcher(a).matches())).collect(Collectors.toList());
        // A protected attribute name is the clear attribute name prefixed by cps1/
        String[][] mapping = attributes.stream()
                .map(a -> new String[] { a,
                        Arrays.stream(fqDataIdPatterns).anyMatch(t -> t.matcher(a).matches()) ? "csp1/" + a : null })
                .toArray(String[][]::new);
        return mapping;
    }

    @Override
    public Promise get(String[] attributeNames, String[] criteria, Operation operation) {
        // TODO workaround to fix type of geometry with coarsening
        int index = Arrays.asList(attributeNames).indexOf("AddGeometryColumn");
        if (index != -1) {
            criteria[index] = criteria[index].replaceAll("POINT", "POLYGON");
        }
        // TODO workaround (promise not yet implemented in module)
        //Promise promise = anonymizeModule.get(attributeNames, criteria, operation);
        DefaultPromise promise = new DefaultPromise();
        promise.setAttributeNames(attributeNames);
        return promise;
    }

    @Override
    public String[][] get(Promise promise, String[][] contents) {
        // TODO workaround (promise not yet implemented in module)
        //return anonymizeModule.get(promise, contents);
        boolean modify = false;
        if (dataIdPatterns != null && promise != null && promise.getAttributeNames() != null) {
            modify = Arrays.stream(promise.getAttributeNames())
                    .anyMatch(an -> Arrays.stream(dataIdPatterns).anyMatch(p -> p.matcher(an).matches()));
        }
        if (modify) {
            return contents.clone();
        }
        return contents;
    }

    @Override
    public String[][] post(String[] attributeNames, String[][] contents) {
        return anonymizeModule.post(attributeNames, contents);
    }

    @Override
    public String[][] put(String[] attributeNames, String[] criteria, String[][] contents) {
        return anonymizeModule.put(attributeNames, criteria, contents);
    }

    @Override
    public void delete(String[] attributeNames, String[] criteria) {
        anonymizeModule.delete(attributeNames, criteria);
    }
}
