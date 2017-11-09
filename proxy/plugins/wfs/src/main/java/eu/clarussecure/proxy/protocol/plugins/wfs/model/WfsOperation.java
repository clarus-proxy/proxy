package eu.clarussecure.proxy.protocol.plugins.wfs.model;

public enum WfsOperation {

    GET_CAPABILITIES("GetCapabilities"),
    DESCRIBE_FEATURE_TYPE("DescribeFeatureType"),
    GET_FEATURE("GetFeature"),
    LOCK_FEATURE("LockFeature"),
    TRANSACTION("Transaction");

    private String name = "";

    WfsOperation(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static WfsOperation valueOfByName(String name) throws IllegalArgumentException {

        for (WfsOperation value : values()) {
            String op = value.getName();
            if (op.equalsIgnoreCase(name)) {
                return value;
            }
        }
        throw new IllegalArgumentException(String.format("operation %s is not defined", name));
    }

}
