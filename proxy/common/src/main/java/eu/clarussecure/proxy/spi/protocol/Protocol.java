package eu.clarussecure.proxy.spi.protocol;

public interface Protocol {

    ProtocolCapabilities getCapabilities();

    Configuration getConfiguration();

    void start();

    void sync();

    void stop();

    default String[] adaptDataIds(String[] dataIds) {
        return dataIds;
    }

    default String[] getDatasetPrefixByServer() {
        return new String[0];
    }
}
