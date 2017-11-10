package eu.clarussecure.proxy.protocol.plugins.wfs.model;

public enum WfsParameter {

    /** General parameters */
    SERVICE("service"),
    VERSION("version"),
    REQUEST("request"),
    TYPE_NAME("typeName"),
    SRS_NAME("srsName"),
    EXCEPTIONS("exceptions"),
    OUTPUT_FORMAT("outputFormat"),

    /** GetFeature parameters */
    FEATURE_ID("featureID"),
    PROPERTY_NAME("propertyName"),
    BBOX("bbox");

    private String parameter = "";

    WfsParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getParameter() {
        return this.parameter;
    }
}
