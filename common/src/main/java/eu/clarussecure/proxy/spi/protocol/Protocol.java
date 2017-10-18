package eu.clarussecure.proxy.spi.protocol;

import java.util.concurrent.ExecutionException;

public interface Protocol {

    ProtocolCapabilities getCapabilities();

    Configuration getConfiguration();

    void start();

    void waitForServerIsReady() throws InterruptedException;

    void sync() throws InterruptedException, ExecutionException;

    void stop();

    default String[] adaptDataIds(String[] dataIds) {
        return dataIds;
    }
}
