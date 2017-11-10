package eu.clarussecure.proxy.protocol.plugins.wfs.model;

import java.util.Arrays;

public enum ProtocolVersion {

    V1_0_0("1.0.0"), V1_1_0("1.1.0"), V2_0_0("2.0.0");

    private String version;

    private ProtocolVersion(String value) {
        this.version = value;
    }

    public String getValue() {
        return version;
    }

    public static ProtocolVersion fromValue(final String value) {
        return Arrays.asList(values()).stream().filter(entry -> entry.getValue().equals(value)).findFirst().get();
    }

    @Override
    public String toString() {
        return this.version;
    }

}