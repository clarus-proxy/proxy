package eu.clarussecure.proxy.spi;

import java.util.stream.Stream;

public enum Operation {
    CREATE, READ, UPDATE, DELETE;

    public static Stream<Operation> stream(Operation... values) {
        return Stream.of(values);
    }
}
