package eu.clarussecure.proxy.spi.protocol;

public interface Protocol {

    ProtocolCapabilities getCapabilities();

    Configuration getConfiguration();

    void start();

    void waitForServerIsReady() throws InterruptedException;

    void sync() throws InterruptedException;

    void stop();

    default String[] adaptDataIds(String[] dataIds) {
        return dataIds;
    }

    default String[] getDatasetPrefixByServer() {
        return new String[0];
    }
}
