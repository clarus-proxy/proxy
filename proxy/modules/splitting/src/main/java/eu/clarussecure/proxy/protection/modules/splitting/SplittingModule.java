package eu.clarussecure.proxy.protection.modules.splitting;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.geotools.geometry.jts.GeometryBuilder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.CRS.AxisOrder;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.w3c.dom.Document;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ByteArrayInStream;
import com.vividsolutions.jts.io.ByteOrderDataInStream;
import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBConstants;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

import eu.clarussecure.dataoperations.DataOperation;
import eu.clarussecure.dataoperations.Operation;
import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.proxy.spi.protection.DefaultPromise;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;

public class SplittingModule implements ProtectionModule, DataOperation {

    private static final String PROTECTION_MODULE_NAME = "Splitting";
    private static final String CSP = "csp";
    private static final Pattern START_WITH_DOUBLE_ASTERISKS = Pattern.compile("^([^/*]*\\*/[^/*]*\\*/)([^/*]*)");
    private static final Pattern START_WITH_SINGLE_ASTERISK = Pattern.compile("^([^/*]*\\*/)([^/*]*/[^/*]*)");

    private String[] dataIds;
    private Pattern[] fqDataIdPatterns;
    private String[] attributeTypes;
    private String[] dataTypes;
    private Map<String, Map<String, String>> protections;
    private Map<Pattern, List<Integer>> dataIdCSPs;
    private String[] datasetPrefixByServer;

    private static class CapabilitiesHelper {
        private static final SplittingCapabilities INSTANCE = new SplittingCapabilities();
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
        if (dataIds != null) {
            // Replace unqualified data ids by a generic qualified id (with
            // asterisks): an1 -> */*/an1, d/an2 -> */d/an2, ds/d/an3 ->
            // ds/d/an3
            dataIds = Arrays.stream(dataIds)
                    .map(id -> id.indexOf('/') == -1
                            // prepend with */*/ if there is no /
                            ? "*/*/" + id
                            : id.indexOf('/') == id.lastIndexOf('/')
                                    // prepend with */ if there is one /
                                    ? "*/" + id
                                    // do nothing if there is two /
                                    : id)
                    .toArray(String[]::new);
            this.dataIds = dataIds;
            this.fqDataIdPatterns = Arrays.stream(dataIds).map(s -> escapeRegex(s)).map(Pattern::compile)
                    .toArray(Pattern[]::new);
            this.attributeTypes = new String[dataIds.length];
            this.dataTypes = new String[dataIds.length];
            this.protections = new HashMap<>();
            this.protections.put("technical_identifier", Stream.of("protection=replicate", "clouds=2")
                    .map(str -> str.split("=")).collect(Collectors.toMap(tk -> tk[0], tk -> tk[1])));
            this.protections.put("identifier", Stream.of("protection=splitting", "clouds=2", "splitting_type=lines")
                    .map(str -> str.split("=")).collect(Collectors.toMap(tk -> tk[0], tk -> tk[1])));
            this.protections.put("non_confidential", Stream.of("protection=splitting", "clouds=1")
                    .map(str -> str.split("=")).collect(Collectors.toMap(tk -> tk[0], tk -> tk[1])));
            this.datasetPrefixByServer = datasetPrefixByServer;
            this.dataIdCSPs = new HashMap<>();
            this.dataIdCSPs.put(fqDataIdPatterns[0], Collections.singletonList(0)); // nom_com
            this.attributeTypes[0] = "non_confidential";
            this.dataTypes[0] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[1], Collections.singletonList(1)); // adresse
            this.attributeTypes[1] = "non_confidential";
            this.dataTypes[1] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[2], Collections.singletonList(0)); // code_bss
            this.attributeTypes[2] = "non_confidential";
            this.dataTypes[2] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[3], Collections.singletonList(1)); // denominati
            this.attributeTypes[3] = "non_confidential";
            this.dataTypes[3] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[4], Collections.singletonList(0)); // type_point
            this.attributeTypes[4] = "non_confidential";
            this.dataTypes[4] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[5], Collections.singletonList(1)); // district
            this.attributeTypes[5] = "non_confidential";
            this.dataTypes[5] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[6], Collections.singletonList(0)); // circonscri
            this.attributeTypes[6] = "non_confidential";
            this.dataTypes[6] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[7], Collections.singletonList(1)); // precision
            this.attributeTypes[7] = "non_confidential";
            this.dataTypes[7] = "categoric";
            this.dataIdCSPs.put(fqDataIdPatterns[8], Collections.singletonList(0)); // altitude
            this.attributeTypes[8] = "non_confidential";
            this.dataTypes[8] = "numeric_continuous";
            this.dataIdCSPs.put(fqDataIdPatterns[9], Collections.singletonList(1)); // prof_max
            this.attributeTypes[9] = "non_confidential";
            this.dataTypes[9] = "numeric_continuous";
            this.dataIdCSPs.put(fqDataIdPatterns[10], Stream.of(0, 1).collect(Collectors.toList())); // geom
            this.attributeTypes[10] = "identifier";
            this.dataTypes[10] = "geometric_object";
            this.dataIdCSPs.put(fqDataIdPatterns[11], Stream.of(0, 1).collect(Collectors.toList())); // gid
            this.attributeTypes[11] = "technical_identifier";
            this.dataTypes[11] = "numeric_continuous";
        }
    }

    @Override
    public DataOperation getDataOperation() {
        return this;
    }

    private String escapeRegex(String regex) {
        return regex.replace(".", "\\.").replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)")
                .replace("*", "[^/]*");
    }

    private String resolveAttribute(String fqAttributeName) {
        String attributeName;
        if (fqAttributeName.indexOf('*') == -1) {
            // Attribute name doesn't contain asterisk (*)
            attributeName = fqAttributeName;
        } else {
            // Replace attribute name that contains asterisk (*) by the matching
            // data identifier
            Pattern attributeNamePattern = Pattern.compile(escapeRegex(fqAttributeName));
            Stream<String> missingDataIds1 = Arrays.stream(dataIds).map(id -> START_WITH_DOUBLE_ASTERISKS.matcher(id))
                    .filter(m -> m.matches()).map(m -> new String[] { m.group(1), m.group(2) }).flatMap(groups -> {
                        Pattern firstPartPattern = Pattern.compile(escapeRegex(groups[0]));
                        String lastPart = groups[1];
                        return Stream.of(fqAttributeName).map(an -> an.substring(0, an.lastIndexOf('/') + 1))
                                .filter(an -> firstPartPattern.matcher(an).matches()).map(an -> an + lastPart);
                    });
            Stream<String> missingDataIds2 = Arrays.stream(dataIds).map(id -> START_WITH_SINGLE_ASTERISK.matcher(id))
                    .filter(m -> m.matches()).map(m -> new String[] { m.group(1), m.group(2) }).flatMap(groups -> {
                        Pattern firstPartPattern = Pattern.compile(escapeRegex(groups[0]));
                        String lastPart = groups[1];
                        return Stream.of(fqAttributeName).map(an -> an.substring(0, an.indexOf('/') + 1))
                                .filter(an -> firstPartPattern.matcher(an).matches()).map(an -> an + lastPart);
                    });
            List<String> dataIds = Stream.concat(missingDataIds1, missingDataIds2).distinct()
                    .collect(Collectors.toList());
            attributeName = dataIds.stream().filter(id -> attributeNamePattern.matcher(id).matches()).findFirst()
                    .orElse(fqAttributeName);
        }
        return attributeName;
    }

    private List<String> resolveAttributes(String[] fqAttributeNames) {
        List<String> attributeNames;
        if (Arrays.stream(fqAttributeNames).filter(an -> an.indexOf('*') != -1).count() == 0) {
            // Attribute names don't contain asterisk (*)
            attributeNames = Arrays.stream(fqAttributeNames).collect(Collectors.toList());
        } else {
            // Replace attribute names that contain asterisk (*) by the matching
            // data identifiers
            List<Map.Entry<String, Pattern>> attributeNamePatterns = Arrays.stream(fqAttributeNames)
                    .map(an -> new SimpleEntry<>(an, Pattern.compile(escapeRegex(an)))).collect(Collectors.toList());
            Stream<String> retainedDataIds = Arrays.stream(dataIds).filter(id -> id.indexOf('*') == -1);
            Stream<String> missingDataIds1 = Arrays.stream(dataIds).map(id -> START_WITH_DOUBLE_ASTERISKS.matcher(id))
                    .filter(m -> m.matches()).map(m -> new String[] { m.group(1), m.group(2) }).flatMap(groups -> {
                        Pattern firstPartPattern = Pattern.compile(escapeRegex(groups[0]));
                        String lastPart = groups[1];
                        return Arrays.stream(fqAttributeNames).map(an -> an.substring(0, an.lastIndexOf('/') + 1))
                                .filter(an -> firstPartPattern.matcher(an).matches()).map(an -> an + lastPart);
                    });
            Stream<String> missingDataIds2 = Arrays.stream(dataIds).map(id -> START_WITH_SINGLE_ASTERISK.matcher(id))
                    .filter(m -> m.matches()).map(m -> new String[] { m.group(1), m.group(2) }).flatMap(groups -> {
                        Pattern firstPartPattern = Pattern.compile(escapeRegex(groups[0]));
                        String lastPart = groups[1];
                        return Arrays.stream(fqAttributeNames).map(an -> an.substring(0, an.indexOf('/') + 1))
                                .filter(an -> firstPartPattern.matcher(an).matches()).map(an -> an + lastPart);
                    });
            List<String> dataIds = Stream.concat(retainedDataIds, Stream.concat(missingDataIds1, missingDataIds2))
                    .distinct().collect(Collectors.toList());
            List<Map.Entry<String, Stream<String>>> resolvedDataIds = attributeNamePatterns.stream()
                    .map(e -> new SimpleEntry<>(e.getKey(),
                            dataIds.stream().filter(id -> e.getValue().matcher(id).matches())))
                    .collect(Collectors.toList());
            List<Map.Entry<String, Stream<String>>> unresolvedAttributeNames = attributeNamePatterns.stream()
                    .filter(e -> dataIds.stream().noneMatch(id -> e.getValue().matcher(id).matches()))
                    .map(e -> new SimpleEntry<>(e.getKey(), Stream.of(e.getKey()))).collect(Collectors.toList());
            // Concatenate all found attributes
            List<Map.Entry<String, Stream<String>>> resolvedAttributeNames = Stream
                    .concat(resolvedDataIds.stream(), unresolvedAttributeNames.stream()).collect(Collectors.toList());
            attributeNames = resolvedAttributeNames.stream().flatMap(Map.Entry::getValue).collect(Collectors.toList());
        }
        return attributeNames;
    }

    private String buildProtectedAttributeName(int csp, String attributeName) {
        if (attributeName.chars().filter(c -> c == '/').count() == 2) {
            if (datasetPrefixByServer != null && csp < datasetPrefixByServer.length) {
                attributeName = datasetPrefixByServer[csp] + attributeName.substring(attributeName.indexOf('/'));
            }
        }
        attributeName = CSP + (csp + 1) + "/" + attributeName;
        return attributeName;
    }

    private List<Map.Entry<String, List<String>>> resolveProtectedAttributes(List<String> attributes) {
        List<Map.Entry<String, List<String>>> mapping = attributes.stream().map(attr -> {
            List<Integer> csps = dataIdCSPs.get(
                    Arrays.stream(fqDataIdPatterns).filter(p -> p.matcher(attr).matches()).findFirst().orElse(null));
            List<String> protectedAttributeNames = csps == null ? Collections.emptyList()
                    : csps.stream().map(csp -> buildProtectedAttributeName(csp, attr)).collect(Collectors.toList());
            return new SimpleEntry<>(attr, protectedAttributeNames);
        }).collect(Collectors.toList());
        return mapping;
    }

    @Override
    public String[][] head(String[] fqAttributeNames) {
        // TODO workaround (not yet implemented in module)
        // Assume attribute names are fully qualified (ds/d/a)
        List<String> attributeNames = resolveAttributes(fqAttributeNames);
        List<Map.Entry<String, List<String>>> protectedAttributeNames = resolveProtectedAttributes(attributeNames);
        String[][] mapping = protectedAttributeNames.stream()
                .map(e -> Stream
                        .concat(Stream.of(e.getKey()),
                                e.getValue().isEmpty() ? Stream.of((String) null) : e.getValue().stream())
                        .toArray(String[]::new))
                .toArray(String[][]::new);
        return mapping;
    }

    @Override
    public Promise get(String[] attributeNames, String[] criteria, Operation operation, boolean dispatch) {
        // TODO workaround (promise not yet implemented in module)
        // Assume attribute names are fully qualified (ds/d/a)
        String[] fqAttributeNames = attributeNames;
        DefaultPromise promise = new DefaultPromise();
        promise.setCriteria(criteria);
        List<String> attributes = resolveAttributes(fqAttributeNames);
        promise.setAttributeNames(attributes.stream().toArray(String[]::new));
        List<Map.Entry<String, List<String>>> resolvedProtectedAttributeNames = resolveProtectedAttributes(attributes);
        List<Integer> involvedCSPs = resolvedProtectedAttributeNames.stream().map(Map.Entry::getValue)
                .flatMap(l -> l.stream()).map(p -> Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1)
                .collect(Collectors.toList());
        if (involvedCSPs.isEmpty()) {
            if (dispatch) {
                involvedCSPs.addAll(dataIdCSPs.values().stream().flatMap(Collection::stream).distinct().sorted()
                        .collect(Collectors.toList()));
            } else {
                involvedCSPs.add(0);
            }
        }
        resolvedProtectedAttributeNames.forEach(e -> {
            if (e.getValue().isEmpty()) {
                e.setValue(involvedCSPs.stream().map(csp -> buildProtectedAttributeName(csp, e.getKey()))
                        .collect(Collectors.toList()));
            }
        });
        List<String> protectedAttributeNamesAsList = resolvedProtectedAttributeNames.stream().map(Map.Entry::getValue)
                .flatMap(l -> l.stream()).collect(Collectors.toList());
        Map<Integer, List<String>> protectedAttributeNamesByCSP = protectedAttributeNamesAsList.stream()
                .collect(Collectors.groupingBy(p -> Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1));
        int maxCSP = protectedAttributeNamesByCSP.keySet().stream().max(Comparator.naturalOrder()).get();
        IntStream.range(0, maxCSP).forEach(csp -> {
            if (protectedAttributeNamesByCSP.get(csp) == null) {
                protectedAttributeNamesByCSP.put(csp, Collections.emptyList());
            }
        });
        String[][] protectedAttributeNames = protectedAttributeNamesByCSP.values().stream()
                .map(l -> l.stream().map(p -> p.substring(p.indexOf('/') + 1)).toArray(String[]::new))
                .toArray(String[][]::new);
        promise.setProtectedAttributeNames(protectedAttributeNames);
        int[] cspPostion = new int[protectedAttributeNames.length];
        int[][][] attributeMapping = IntStream.range(0, attributes.size())
                .mapToObj(i -> resolvedProtectedAttributeNames.get(i).getValue().stream().map(p -> {
                    int csp = Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1;
                    int idx = cspPostion[csp]++;
                    return new int[] { csp, idx };
                }).toArray(int[][]::new)).toArray(int[][][]::new);
        promise.setAttributeMapping(attributeMapping);
        List<Map.Entry<String, String>> criteriaAttributeNameToValue = Arrays.asList(criteria).stream()
                .map(c -> c.split("=clarus_equals=")).map(tk -> new SimpleEntry<>(resolveAttribute(tk[0]), tk[1]))
                .collect(Collectors.toList());
        List<Map.Entry<String, List<String>>> resolvedProtectedCriteriaAttributeNames = resolveProtectedAttributes(
                criteriaAttributeNameToValue.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
        resolvedProtectedCriteriaAttributeNames.forEach(e -> {
            if (e.getValue().isEmpty()) {
                e.setValue(Collections.singletonList(buildProtectedAttributeName(0, e.getKey())));
            }
        });
        List<Map.Entry<String, String>> protectedCriteriaAttributeNamesToCriteria = IntStream
                .range(0, criteriaAttributeNameToValue.size()).boxed().flatMap(i -> {
                    String criteriaValue = criteriaAttributeNameToValue.get(i).getValue();
                    List<String> protectedCriteriaAttributeNames = resolvedProtectedCriteriaAttributeNames.get(i)
                            .getValue();
                    return protectedCriteriaAttributeNames.stream().map(p -> new SimpleEntry<>(p, criteriaValue));
                }).collect(Collectors.toList());
        Map<Integer, List<Map.Entry<String, String>>> protectedCriteriaByCSP = protectedCriteriaAttributeNamesToCriteria
                .stream().collect(Collectors.groupingBy(
                        e -> Integer.valueOf(e.getKey().substring(CSP.length(), e.getKey().indexOf('/'))) - 1));
        String[][] protectedCriteria = protectedCriteriaByCSP.entrySet().stream()
                .map(e -> e.getValue().stream()
                        .map(c -> c.getKey().substring(c.getKey().indexOf('/') + 1) + "=clarus_equals=" + c.getValue())
                        .toArray(String[]::new))
                .toArray(String[][]::new);
        promise.setProtectedCriteria(protectedCriteria);
        return promise;
    }

    @Override
    public String[][] get(Promise promise, String[][] contents) {
        // TODO workaround (promise not yet implemented in module)
        // assume contents is an array of one row by csp
        int[][][] attributeMapping = ((DefaultPromise) promise).getAttributeMapping();
        String[] attributeNames = ((DefaultPromise) promise).getAttributeNames();
        String[] newContent = IntStream.range(0, attributeMapping.length).mapToObj(i -> {
            int j = IntStream.range(0, fqDataIdPatterns.length).boxed()
                    .filter(k -> fqDataIdPatterns[k].matcher(attributeNames[i]).matches()).findFirst().orElse(-1);
            return merge(contents, attributeMapping[i], j != -1 ? attributeTypes[j] : null,
                    j != -1 ? dataTypes[j] : null);
        }).toArray(String[]::new);
        String[][] newContents = new String[][] { newContent };
        return newContents;
    }

    private String merge(String[][] contents, int[][] indexes, String attributeType, String dataType) {
        if (attributeType == null) {
            int[] index = indexes[0];
            int csp = index[0];
            int idx = index[1];
            return contents[csp][idx];
        }
        Map<String, String> protectionParameters = protections.get(attributeType);
        String protection = protectionParameters.get("protection");
        Integer nbClouds = Integer.parseInt(protectionParameters.get("clouds"));
        String splittingType = protectionParameters.get("splitting_type");
        if ("splitting".equals(protection)) {
            if (nbClouds == 1) {
                if (indexes.length == 1) {
                    int[] index = indexes[0];
                    int csp = index[0];
                    int idx = index[1];
                    return contents[csp][idx];
                } else {
                    throw new IllegalStateException("unexpected");
                }
            } else if (nbClouds == 2) {
                String[] splittedContent = IntStream.range(0, indexes.length).boxed().map(i -> {
                    int[] index = indexes[i];
                    int csp = index[0];
                    int idx = index[1];
                    return contents[csp][idx];
                }).toArray(String[]::new);
                if (splittedContent.length == 2) {
                    if ("geometric_object".equals(dataType)) {
                        if ("lines".equals(splittingType)) {
                            return linesToPoint(splittedContent[0], splittedContent[1]);
                        } else {
                            throw new IllegalStateException("unexpected");
                        }
                    } else {
                        throw new IllegalStateException("unexpected");
                    }
                } else {
                    throw new IllegalStateException("unexpected");
                }
            } else {
                throw new IllegalStateException("unexpected");
            }
        } else if ("replicate".equals(protection)) {
            if (nbClouds == 2) {
                String[] splittedContent = IntStream.range(0, indexes.length).boxed().map(i -> {
                    int[] index = indexes[i];
                    int csp = index[0];
                    int idx = index[1];
                    return contents[csp][idx];
                }).toArray(String[]::new);
                if (splittedContent.length == 2) {
                    return splittedContent[0];
                } else {
                    throw new IllegalStateException("unexpected");
                }
            } else {
                throw new IllegalStateException("unexpected");
            }
        } else {
            throw new IllegalStateException("unexpected");
        }
    }

    private static String linesToPoint(String wkLine1, String wkLine2) {
        boolean withSRID = false;
        boolean wkt = false;
        boolean wkb = false;

        int srid = 0;
        if (wkLine1.startsWith("SRID")) {
            int begin = wkLine1.indexOf('=') + 1;
            int end = wkLine1.indexOf(';', begin);
            srid = Integer.parseInt(wkLine1.substring(begin, end));
            wkLine1 = wkLine1.substring(end + 1);
            withSRID = true;
        }
        if (wkLine2.startsWith("SRID")) {
            int begin = wkLine2.indexOf('=') + 1;
            int end = wkLine2.indexOf(';', begin);
            srid = Integer.parseInt(wkLine2.substring(begin, end));
            wkLine2 = wkLine2.substring(end + 1);
            withSRID = true;
        }
        LineString line1 = null;
        LineString line2 = null;
        int byteOrder = ByteOrderValues.BIG_ENDIAN;
        boolean hasSRID = false;
        try {
            WKTReader reader = new WKTReader();
            line1 = (LineString) reader.read(wkLine1);
            line2 = (LineString) reader.read(wkLine2);

            line1.setSRID(srid);
            line2.setSRID(srid);

            wkt = true;
        } catch (ParseException e) {
            try {
                WKBReader reader = new WKBReader();
                byte[] bytes1 = WKBReader.hexToBytes(wkLine1);
                line1 = (LineString) reader.read(bytes1);
                byte[] bytes2 = WKBReader.hexToBytes(wkLine2);
                line2 = (LineString) reader.read(bytes2);

                ByteArrayInStream bin = new ByteArrayInStream(bytes1);
                ByteOrderDataInStream dis = new ByteOrderDataInStream(bin);
                byte byteOrderWKB = dis.readByte();
                byteOrder = byteOrderWKB == WKBConstants.wkbNDR ? ByteOrderValues.LITTLE_ENDIAN
                        : ByteOrderValues.BIG_ENDIAN;
                dis.setOrder(byteOrder);
                int typeInt = dis.readInt();
                hasSRID = (typeInt & 0x20000000) != 0;

                wkb = true;
            } catch (ParseException | IOException e2) {
                // should not occur
            }
        }

        String wkPoint;
        if (wkt || wkb) {
            Point point = (Point) line1.intersection(line2);
            point.setSRID(line1.getSRID());

            if (wkt) {
                WKTWriter writer = new WKTWriter();
                wkPoint = writer.write(point);
                if (withSRID) {
                    wkPoint = "SRID=" + point.getSRID() + ";" + wkPoint;
                }
            } else /* if (wkb) */ {
                WKBWriter writer = new WKBWriter(2, byteOrder, hasSRID);
                wkPoint = WKBWriter.toHex(writer.write(point));
            }
        } else {
            wkPoint = wkLine1;
        }

        return wkPoint;
    }

    @Override
    public String[][][] post(String[] attributeNames, String[][] contents) {
        String[] fqAttributeNames = attributeNames;
        List<String> attributes = resolveAttributes(fqAttributeNames);
        List<Map.Entry<String, List<String>>> resolvedProtectedAttributeNames = resolveProtectedAttributes(attributes);
        resolvedProtectedAttributeNames.forEach(e -> {
            if (e.getValue().isEmpty()) {
                e.setValue(Collections.singletonList(buildProtectedAttributeName(0, e.getKey())));
            }
        });
        List<String> protectedAttributeNamesAsList = resolvedProtectedAttributeNames.stream().map(Map.Entry::getValue)
                .flatMap(l -> l.stream()).collect(Collectors.toList());
        Map<Integer, List<String>> protectedAttributeNamesByCSP = protectedAttributeNamesAsList.stream()
                .collect(Collectors.groupingBy(p -> Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1));
        int maxCSP = protectedAttributeNamesByCSP.keySet().stream().max(Comparator.naturalOrder()).get();
        IntStream.range(0, maxCSP + 1).forEach(csp -> {
            if (protectedAttributeNamesByCSP.get(csp) == null) {
                protectedAttributeNamesByCSP.put(csp, Collections.emptyList());
            }
        });
        String[][] protectedAttributeNames = protectedAttributeNamesByCSP.values().stream()
                .map(l -> l.stream().map(p -> p.substring(p.indexOf('/') + 1)).toArray(String[]::new))
                .toArray(String[][]::new);
        int[] cspPostion = new int[protectedAttributeNames.length];
        // build mapping between clear attributes and protected attributes
        // [clear idx][csp][protected idx]
        int[][][] attributeMapping = IntStream.range(0, attributes.size())
                .mapToObj(i -> resolvedProtectedAttributeNames.get(i).getValue().stream().map(p -> {
                    int csp = Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1;
                    int idx = cspPostion[csp]++;
                    return new int[] { csp, idx };
                }).toArray(int[][]::new)).toArray(int[][][]::new);
        // allocate new content array [csp][row][value]: an attribute value per
        // row per csp
        String[][][] newContents = IntStream.range(0, protectedAttributeNames.length)
                .mapToObj(csp -> IntStream.range(0, 1 + contents.length)
                        .mapToObj(r -> new String[protectedAttributeNames[csp].length]).toArray(String[][]::new))
                .toArray(String[][][]::new);
        // first row contains the protected attribute names
        for (int csp = 0; csp < protectedAttributeNames.length; csp++) {
            for (int idx = 0; idx < protectedAttributeNames[csp].length; idx++) {
                newContents[csp][0][idx] = protectedAttributeNames[csp][idx];
            }
        }
        // split values
        IntStream.range(0, contents.length).forEach(r -> IntStream.range(0, attributeMapping.length).forEach(i -> {
            int j = IntStream.range(0, fqDataIdPatterns.length).boxed()
                    .filter(k -> fqDataIdPatterns[k].matcher(attributeNames[i]).matches()).findFirst().orElse(-1);
            split(contents, r, i, newContents, attributeMapping[i], j != -1 ? attributeTypes[j] : null,
                    j != -1 ? dataTypes[j] : null);
        }));
        return newContents;
    }

    @Override
    public String[][][] put(String[] attributeNames, String[] criteria, String[][] contents) {
        String[] fqAttributeNames = attributeNames;
        List<String> attributes = resolveAttributes(fqAttributeNames);
        List<Map.Entry<String, List<String>>> resolvedProtectedAttributeNames = resolveProtectedAttributes(attributes);
        resolvedProtectedAttributeNames.forEach(e -> {
            if (e.getValue().isEmpty()) {
                e.setValue(Collections.singletonList(buildProtectedAttributeName(0, e.getKey())));
            }
        });
        List<String> protectedAttributeNamesAsList = resolvedProtectedAttributeNames.stream().map(Map.Entry::getValue)
                .flatMap(l -> l.stream()).collect(Collectors.toList());
        Map<Integer, List<String>> protectedAttributeNamesByCSP = protectedAttributeNamesAsList.stream()
                .collect(Collectors.groupingBy(p -> Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1));
        int maxCSP = protectedAttributeNamesByCSP.keySet().stream().max(Comparator.naturalOrder()).get();
        IntStream.range(0, maxCSP).forEach(csp -> {
            if (protectedAttributeNamesByCSP.get(csp) == null) {
                protectedAttributeNamesByCSP.put(csp, Collections.emptyList());
            }
        });
        String[][] protectedAttributeNames = protectedAttributeNamesByCSP.values().stream()
                .map(l -> l.stream().map(p -> p.substring(p.indexOf('/') + 1)).toArray(String[]::new))
                .toArray(String[][]::new);
        int[] cspPostion = new int[protectedAttributeNames.length];
        // build mapping between clear attributes and protected attributes
        // [clear idx][csp][protected idx]
        int[][][] attributeMapping = IntStream.range(0, attributes.size())
                .mapToObj(i -> resolvedProtectedAttributeNames.get(i).getValue().stream().map(p -> {
                    int csp = Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1;
                    int idx = cspPostion[csp]++;
                    return new int[] { csp, idx };
                }).toArray(int[][]::new)).toArray(int[][][]::new);
        // process criteria
        if (criteria != null && criteria.length > 0) {
            List<Map.Entry<String, String>> criteriaAttributeNameToValue = Arrays.asList(criteria).stream()
                    .map(c -> c.split("=clarus_equals=")).map(tk -> new SimpleEntry<>(resolveAttribute(tk[0]), tk[1]))
                    .collect(Collectors.toList());
            List<Map.Entry<String, List<String>>> resolvedProtectedCriteriaAttributeNames = resolveProtectedAttributes(
                    criteriaAttributeNameToValue.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
            resolvedProtectedCriteriaAttributeNames.forEach(e -> {
                if (e.getValue().isEmpty()) {
                    e.setValue(Collections.singletonList(buildProtectedAttributeName(0, e.getKey())));
                }
            });
            List<Map.Entry<String, String>> protectedCriteriaAttributeNamesToCriteria = IntStream
                    .range(0, criteriaAttributeNameToValue.size()).boxed().flatMap(i -> {
                        String criteriaValue = criteriaAttributeNameToValue.get(i).getValue();
                        List<String> protectedCriteriaAttributeNames = resolvedProtectedCriteriaAttributeNames.get(i)
                                .getValue();
                        return protectedCriteriaAttributeNames.stream().map(p -> new SimpleEntry<>(p, criteriaValue));
                    }).collect(Collectors.toList());
            Map<Integer, List<Map.Entry<String, String>>> protectedCriteriaByCSP = protectedCriteriaAttributeNamesToCriteria
                    .stream().collect(Collectors.groupingBy(
                            e -> Integer.valueOf(e.getKey().substring(CSP.length(), e.getKey().indexOf('/'))) - 1));
            String[][] protectedCriteria = protectedCriteriaByCSP.entrySet()
                    .stream().map(
                            e -> e.getValue()
                                    .stream().map(c -> c.getKey().substring(c.getKey().indexOf('/') + 1)
                                            + "=clarus_equals=" + c.getValue())
                                    .toArray(String[]::new))
                    .toArray(String[][]::new);
            // save only 1st protected criteria
            System.arraycopy(protectedCriteria[0], 0, criteria, 0, protectedCriteria[0].length);
        }
        // allocate new content array [csp][row][value]: an attribute value per
        // row per csp
        String[][][] newContents = IntStream.range(0, protectedAttributeNames.length)
                .mapToObj(csp -> IntStream.range(0, 1 + contents.length)
                        .mapToObj(r -> new String[protectedAttributeNames[csp].length]).toArray(String[][]::new))
                .toArray(String[][][]::new);
        // first row contains the protected attribute names
        for (int csp = 0; csp < protectedAttributeNames.length; csp++) {
            for (int idx = 0; idx < protectedAttributeNames[csp].length; idx++) {
                newContents[csp][0][idx] = protectedAttributeNames[csp][idx];
            }
        }
        // split values
        IntStream.range(0, contents.length).forEach(r -> IntStream.range(0, attributeMapping.length).forEach(i -> {
            int j = IntStream.range(0, fqDataIdPatterns.length).boxed()
                    .filter(k -> fqDataIdPatterns[k].matcher(attributeNames[i]).matches()).findFirst().orElse(-1);
            split(contents, r, i, newContents, attributeMapping[i], j != -1 ? attributeTypes[j] : null,
                    j != -1 ? dataTypes[j] : null);
        }));
        return newContents;
    }

    private void split(String[][] contents, int row, int indice, String[][][] newContents, int[][] indexes,
            String attributeType, String dataType) {
        if (attributeType == null) {
            int[] index = indexes[0];
            int csp = index[0];
            int idx = index[1];
            newContents[csp][row + 1][idx] = contents[row][indice];
            return;
        }
        Map<String, String> protectionParameters = protections.get(attributeType);
        String protection = protectionParameters.get("protection");
        Integer nbClouds = Integer.parseInt(protectionParameters.get("clouds"));
        String splittingType = protectionParameters.get("splitting_type");
        if ("splitting".equals(protection)) {
            if (nbClouds == 1) {
                if (indexes.length == 1) {
                    int[] index = indexes[0];
                    int csp = index[0];
                    int idx = index[1];
                    newContents[csp][row + 1][idx] = contents[row][indice];
                } else {
                    throw new IllegalStateException("unexpected");
                }
            } else if (nbClouds == 2) {
                if ("geometric_object".equals(dataType)) {
                    if ("lines".equals(splittingType)) {
                        String[] lines = pointToLines(contents[row][indice]);
                        IntStream.range(0, indexes.length).forEach(i -> {
                            int[] index = indexes[i];
                            int csp = index[0];
                            int idx = index[1];
                            newContents[csp][row + 1][idx] = lines[csp];
                        });
                    } else {
                        throw new IllegalStateException("unexpected");
                    }
                } else {
                    throw new IllegalStateException("unexpected");
                }
            } else {
                throw new IllegalStateException("unexpected");
            }
        } else if ("replicate".equals(protection)) {
            if (nbClouds == 2) {
                IntStream.range(0, indexes.length).forEach(i -> {
                    int[] index = indexes[i];
                    int csp = index[0];
                    int idx = index[1];
                    newContents[csp][row + 1][idx] = contents[row][indice];
                });
            } else {
                throw new IllegalStateException("unexpected");
            }
        } else {
            throw new IllegalStateException("unexpected");
        }
    }

    private static String[] pointToLines(String wktPoint) {
        String[] wktLineStrings;
        try {
            int srid = 0;
            if (wktPoint.startsWith("SRID")) {
                int begin = wktPoint.indexOf('=') + 1;
                int end = wktPoint.indexOf(';', begin);
                srid = Integer.parseInt(wktPoint.substring(begin, end));
                wktPoint = wktPoint.substring(end + 1);
            }

            WKTReader reader = new WKTReader();
            Point point = (Point) reader.read(wktPoint);
            point.setSRID(srid);

            CoordinateReferenceSystem crs = CRS.decode("EPSG:" + srid);
            Envelope envelope = CRS.getEnvelope(crs);
            AxisOrder axisOrder = CRS.getAxisOrder(crs);

            LineString horizontalLine;
            LineString verticalLine;
            GeometryBuilder geomBuilder = new GeometryBuilder(new GeometryFactory(new PrecisionModel()));
            if (axisOrder == AxisOrder.EAST_NORTH) {
                horizontalLine = geomBuilder.lineString(envelope.getMinimum(0), point.getY(), envelope.getMaximum(0),
                        point.getY());
                verticalLine = geomBuilder.lineString(point.getX(), envelope.getMinimum(1), point.getX(),
                        envelope.getMaximum(1));
            } else {
                horizontalLine = geomBuilder.lineString(envelope.getMinimum(0), point.getY(), envelope.getMaximum(0),
                        point.getY());
                verticalLine = geomBuilder.lineString(point.getX(), envelope.getMinimum(1), point.getX(),
                        envelope.getMaximum(1));
            }
            horizontalLine.setSRID(srid);
            verticalLine.setSRID(srid);

            WKTWriter writer = new WKTWriter();
            String wktHorizontalLine = "SRID=" + srid + ";" + writer.write(horizontalLine);
            String wktVerticalLine = "SRID=" + srid + ";" + writer.write(verticalLine);
            wktLineStrings = new String[] { wktHorizontalLine, wktVerticalLine };
        } catch (ParseException | FactoryException e) {
            e.printStackTrace();
            wktLineStrings = new String[] { wktPoint, wktPoint };
        }

        return wktLineStrings;
    }

    @Override
    public String[][] delete(String[] attributeNames, String[] criteria) {
        String[] fqAttributeNames = attributeNames;
        List<String> attributes = resolveAttributes(fqAttributeNames);
        List<Map.Entry<String, List<String>>> resolvedProtectedAttributeNames = resolveProtectedAttributes(attributes);
        resolvedProtectedAttributeNames.forEach(e -> {
            if (e.getValue().isEmpty()) {
                e.setValue(Collections.singletonList(buildProtectedAttributeName(0, e.getKey())));
            }
        });
        List<String> protectedAttributeNamesAsList = resolvedProtectedAttributeNames.stream().map(Map.Entry::getValue)
                .flatMap(l -> l.stream()).collect(Collectors.toList());
        Map<Integer, List<String>> protectedAttributeNamesByCSP = protectedAttributeNamesAsList.stream()
                .collect(Collectors.groupingBy(p -> Integer.valueOf(p.substring(CSP.length(), p.indexOf('/'))) - 1));
        int maxCSP = protectedAttributeNamesByCSP.keySet().stream().max(Comparator.naturalOrder()).get();
        IntStream.range(0, maxCSP).forEach(csp -> {
            if (protectedAttributeNamesByCSP.get(csp) == null) {
                protectedAttributeNamesByCSP.put(csp, Collections.emptyList());
            }
        });
        String[][] protectedAttributeNames = protectedAttributeNamesByCSP.values().stream()
                .map(l -> l.stream().map(p -> p.substring(p.indexOf('/') + 1)).toArray(String[]::new))
                .toArray(String[][]::new);
        // process criteria
        if (criteria != null && criteria.length > 0) {
            List<Map.Entry<String, String>> criteriaAttributeNameToValue = Arrays.asList(criteria).stream()
                    .map(c -> c.split("=clarus_equals=")).map(tk -> new SimpleEntry<>(resolveAttribute(tk[0]), tk[1]))
                    .collect(Collectors.toList());
            List<Map.Entry<String, List<String>>> resolvedProtectedCriteriaAttributeNames = resolveProtectedAttributes(
                    criteriaAttributeNameToValue.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
            resolvedProtectedCriteriaAttributeNames.forEach(e -> {
                if (e.getValue().isEmpty()) {
                    e.setValue(Collections.singletonList(buildProtectedAttributeName(0, e.getKey())));
                }
            });
            List<Map.Entry<String, String>> protectedCriteriaAttributeNamesToCriteria = IntStream
                    .range(0, criteriaAttributeNameToValue.size()).boxed().flatMap(i -> {
                        String criteriaValue = criteriaAttributeNameToValue.get(i).getValue();
                        List<String> protectedCriteriaAttributeNames = resolvedProtectedCriteriaAttributeNames.get(i)
                                .getValue();
                        return protectedCriteriaAttributeNames.stream().map(p -> new SimpleEntry<>(p, criteriaValue));
                    }).collect(Collectors.toList());
            Map<Integer, List<Map.Entry<String, String>>> protectedCriteriaByCSP = protectedCriteriaAttributeNamesToCriteria
                    .stream().collect(Collectors.groupingBy(
                            e -> Integer.valueOf(e.getKey().substring(CSP.length(), e.getKey().indexOf('/'))) - 1));
            String[][] protectedCriteria = protectedCriteriaByCSP.entrySet()
                    .stream().map(
                            e -> e.getValue()
                                    .stream().map(c -> c.getKey().substring(c.getKey().indexOf('/') + 1)
                                            + "=clarus_equals=" + c.getValue())
                                    .toArray(String[]::new))
                    .toArray(String[][]::new);
            // save only 1st protected criteria
            System.arraycopy(protectedCriteria[0], 0, criteria, 0, protectedCriteria[0].length);
        }
        return protectedAttributeNames;
    }
}
