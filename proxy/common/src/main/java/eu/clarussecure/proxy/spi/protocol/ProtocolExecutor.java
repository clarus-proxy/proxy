package eu.clarussecure.proxy.spi.protocol;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class ProtocolExecutor implements Protocol {

    private Future<Void> future;

    @Override
    public void start() {
        Callable<Void> server = buildServer();
        try {
            future = Executors.newSingleThreadExecutor().submit(server);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract Callable<Void> buildServer();

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
