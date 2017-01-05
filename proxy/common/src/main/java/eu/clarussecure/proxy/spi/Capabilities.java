package eu.clarussecure.proxy.spi;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Capabilities {

    Set<Operation> getSupportedCRUDOperations(boolean wholeDataset);
    
    Set<Mode> getSupportedProcessingModes(boolean wholeDataset, Operation operation);

    public static Map<Operation, Set<Mode>> toMap(Enum<?>[]... rawOperations) {
        Function<? super Enum<?>[], Stream<SimpleEntry<Operation, Set<Mode>>>> toMapEntry = e -> {
            Collector<Mode, ?, Set<Mode>> toSet = Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet);
            return Stream.of(new AbstractMap.SimpleEntry<>((Operation) (e[0]), Arrays.stream(Arrays.copyOfRange(e, 1, e.length, Mode[].class)).collect(toSet)));
        };
        Collector<SimpleEntry<Operation, Set<Mode>>, ?, Map<Operation, Set<Mode>>> toMap = Collectors.collectingAndThen(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue()), Collections::unmodifiableMap);
        return Stream.of(rawOperations).flatMap(toMapEntry).collect(toMap);
    }
}
