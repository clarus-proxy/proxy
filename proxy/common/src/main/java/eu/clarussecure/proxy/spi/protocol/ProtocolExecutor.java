package eu.clarussecure.proxy.spi.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class ProtocolExecutor implements Protocol, Closeable {

    private Callable<Void> server;
    private Future<Void> future;

    @Override
    public void start() {
        server = buildServer();
        try {
            future = Executors.newSingleThreadExecutor().submit(server);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract Callable<Void> buildServer();

    @Override
    public void waitForServerIsReady() throws InterruptedException {
        if (!(server instanceof ProtocolServer)) {
            throw new UnsupportedOperationException(
                    "the underlying server does not implement the ProtocolServer interface");
        }
        ((ProtocolServer) server).waitForServerIsReady();
    }

    @Override
    public void sync() throws InterruptedException {
        try {
            future.get();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        future.cancel(true);
    }

    @Override
    public void close() throws IOException {
        stop();
    }
}
