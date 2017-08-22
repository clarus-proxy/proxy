package eu.clarussecure.proxy.protection.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.ExtractedArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.store.HttpProxyFactory;
import de.flapdoodle.embed.process.extract.ITempNaming;
import de.flapdoodle.embed.process.extract.UUIDTempNaming;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.runtime.Network;

public class EmbeddedMongoDB {

    private final String host;
    private final int port;
    private MongodExecutable mongodExecutable;
    private MongodProcess mongodProcess;

    public EmbeddedMongoDB(String url) {
        String[] tokens = url.split(":");
        this.host = tokens[0];
        this.port = Integer.parseInt(tokens[1]);
    }

    public EmbeddedMongoDB(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() throws UnknownHostException, IOException {
        IRuntimeConfig runtimeConfig = null;
        if (runtimeConfig == null) {
            String artifactStorePath = System.getProperty("mongodb.artifact.store.path");
            if (artifactStorePath != null) {
                IDirectory artifactStoreDirectoryPath = new FixedPath(artifactStorePath);
                ITempNaming executableNaming = new UUIDTempNaming();
                Command command = Command.MongoD;

                runtimeConfig = new RuntimeConfigBuilder().defaults(command)
                        .artifactStore(new ExtractedArtifactStoreBuilder().defaults(command)
                                .download(new DownloadConfigBuilder().defaultsForCommand(command)
                                        .artifactStorePath(artifactStoreDirectoryPath).build())
                                .executableNaming(executableNaming))
                        .build();
            }
        }
        if (runtimeConfig == null) {
            String hostName = System.getProperty("http.proxyHost", System.getProperty("https.proxyHost"));
            if (hostName != null) {
                int port = Integer
                        .parseInt(System.getProperty("http.proxyPort", System.getProperty("https.proxyPort")));
                Command command = Command.MongoD;
                runtimeConfig = new RuntimeConfigBuilder().defaults(command)
                        .artifactStore(new ExtractedArtifactStoreBuilder().defaults(command)
                                .download(new DownloadConfigBuilder().defaultsForCommand(command)
                                        .proxyFactory(new HttpProxyFactory(hostName, port)).build()))
                        .build();
            }
        }
        if (runtimeConfig == null) {
            runtimeConfig = new RuntimeConfigBuilder().defaults(Command.MongoD).build();
        }
        MongodStarter starter = MongodStarter.getInstance(runtimeConfig);

        mongodExecutable = starter.prepare(new MongodConfigBuilder().version(Version.Main.PRODUCTION)
                .net(new Net(host, port, Network.localhostIsIPv6())).build());
        mongodProcess = mongodExecutable.start();
    }

    public void stop() {
        if (mongodProcess != null) {
            mongodProcess.stop();
        }
        if (mongodExecutable != null) {
            mongodExecutable.stop();
        }
    }
}
