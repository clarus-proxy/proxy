package eu.clarussecure.proxy.protocol.plugins.wfs.model;

import java.util.Arrays;

public enum TransactionOperation {

    INSERT("Insert"), UPDATE("Update"), DELETE("Delete");

    private String value;

    private TransactionOperation(String value) {
        this.value = value;
    }

    private String getValue() {
        return this.value;
    }

    public static TransactionOperation fromValue(final String value) {
        return Arrays.asList(values()).stream().filter(entry -> entry.getValue().equals(value)).findFirst().get();
    }

    @Override
    public String toString() {
        return this.value;
    }

}
