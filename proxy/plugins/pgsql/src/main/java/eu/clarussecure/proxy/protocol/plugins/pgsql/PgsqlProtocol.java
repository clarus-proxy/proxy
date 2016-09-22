package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.Protocol;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

public class PgsqlProtocol implements Protocol {

    private static class CapabilitiesHelper {
        private static final PgsqlCapabilities INSTANCE = new PgsqlCapabilities();
    }

    @Override
    public ProtocolCapabilities getCapabilities() {
        return CapabilitiesHelper.INSTANCE;
    }

    private final PgsqlConfiguration configuration = new PgsqlConfiguration(CapabilitiesHelper.INSTANCE);

    private Future<Void> future;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void start() {
        PgsqlServer pgsqlServer = new PgsqlServer((PgsqlConfiguration) getConfiguration());
        try {
            future = Executors.newSingleThreadExecutor().submit(pgsqlServer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sync() {
        try {
            future.get();
        } catch (ExecutionException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        future.cancel(true);
    }

}
