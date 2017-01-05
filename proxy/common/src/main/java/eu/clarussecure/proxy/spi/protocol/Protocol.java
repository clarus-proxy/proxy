package eu.clarussecure.proxy.spi.protocol;

public interface Protocol {

    ProtocolCapabilities getCapabilities();

    Configuration getConfiguration();

    void start();

    void sync();

    void stop();
}
