package eu.clarussecure.proxy.spi.protocol;

import java.util.concurrent.Callable;

public interface ProtocolServer extends Callable<Void> {
    void waitForServerIsReady() throws InterruptedException;
}
