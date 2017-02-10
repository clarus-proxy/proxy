package eu.clarussecure.proxy.protocol.plugins.tcp.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.Future;

public class SSLSessionInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLSessionInitializer.class);

    public static enum SSLMode {
        DISABLED, ALLOWED, REQUIRED;
    }

    private static SSLMode CLIENT_SSL_MODE;
    private static SSLMode SERVER_SSL_MODE;
    private static final boolean USE_SELF_SIGNED_CERTIFICATE;
    private static final File CERTIFICATE_FILE;
    private static final File PRIVATE_KEY_FILE;
    static {
        String clientSSLMode = System.getProperty("tcp.ssl.client", SSLMode.ALLOWED.toString());
        CLIENT_SSL_MODE = SSLMode.valueOf(clientSSLMode.toUpperCase());
        String serverSSLMode = System.getProperty("tcp.ssl.server", SSLMode.ALLOWED.toString());
        SERVER_SSL_MODE = SSLMode.valueOf(serverSSLMode.toUpperCase());
        String useSelfSignedCertificate = System.getProperty("tcp.ssl.use.self.signed.certificate", "true");
        USE_SELF_SIGNED_CERTIFICATE = Boolean.TRUE.toString().equalsIgnoreCase(useSelfSignedCertificate)
                || "1".equalsIgnoreCase(useSelfSignedCertificate) || "yes".equalsIgnoreCase(useSelfSignedCertificate)
                || "on".equalsIgnoreCase(useSelfSignedCertificate);
        String certificateFilename = System.getProperty("tcp.ssl.certificate.file");
        CERTIFICATE_FILE = certificateFilename == null ? null : new File(certificateFilename);
        String privateKeyFilename = System.getProperty("tcp.ssl.private.key.file");
        PRIVATE_KEY_FILE = privateKeyFilename == null ? null : new File(privateKeyFilename);
    }

    public SSLSessionInitializer() {
    }

    public SSLMode getClientMode() {
        return CLIENT_SSL_MODE;
    }

    public SSLMode getServerMode() {
        return SERVER_SSL_MODE;
    }

    public static void setClientMode(SSLMode clientModeSLL) {
        CLIENT_SSL_MODE = clientModeSLL;
    }

    public static void setServerMode(SSLMode serverModeSLL) {
        SERVER_SSL_MODE = serverModeSLL;
    }

    public Future<Channel> addSSLHandlerOnClientSide(ChannelHandlerContext ctx) throws IOException {
        return addSSLHandlerOnClientSide(ctx, ctx.pipeline());
    }

    public Future<Channel> addSSLHandlerOnClientSide(ChannelHandlerContext ctx, ChannelPipeline pipeline)
            throws IOException {
        SslContextBuilder sslContextBuilder;
        LOGGER.debug("Adding a SSL handler on client side...");
        LOGGER.trace("Building a server SSL context for client side...");
        if (USE_SELF_SIGNED_CERTIFICATE) {
            LOGGER.trace("... using self signed certificate");
            SelfSignedCertificate ssc;
            try {
                ssc = new SelfSignedCertificate();
            } catch (CertificateException e) {
                throw new IOException(e);
            }
            sslContextBuilder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
        } else if (CERTIFICATE_FILE != null && PRIVATE_KEY_FILE != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("... using certificate {} and private key {}", CERTIFICATE_FILE, PRIVATE_KEY_FILE);
            }
            sslContextBuilder = SslContextBuilder.forServer(CERTIFICATE_FILE, PRIVATE_KEY_FILE);
        } else {
            KeyManagerFactory keyManagerFactory;
            try {
                String keyStore = System.getProperty("javax.net.ssl.keyStore",
                        System.getProperty("java.home") + "/lib/security/jssecacerts");
                String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword", "");
                String keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("... using keystore {} (of type {})", keyStore, keyStoreType);
                }
                KeyStore ks = KeyStore.getInstance(keyStoreType);
                char[] password = keyStorePassword.toCharArray();
                try (FileInputStream fis = new FileInputStream(keyStore)) {
                    ks.load(fis, password);
                } catch (CertificateException e) {
                    throw new IOException(e);
                }
                keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(ks, password);
            } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
                throw new IOException(e);
            }
            sslContextBuilder = SslContextBuilder.forServer(keyManagerFactory);
        }
        SslContext sslContextForClientSide = sslContextBuilder.build();
        SSLEngine sslEngine = sslContextForClientSide.newEngine(ctx.alloc());
        SslHandler sslHandler = new SslHandler(sslEngine, true);
        pipeline.addFirst("SSLHandler", sslHandler);
        Future<Channel> handshakeFuture = sslHandler.handshakeFuture();
        LOGGER.debug("SSL handler added SSL on client side");
        return handshakeFuture;
    }

    public Future<Channel> addSSLHandlerOnServerSide(ChannelHandlerContext ctx) throws SSLException {
        return addSSLHandlerOnServerSide(ctx, ctx.pipeline());
    }

    public Future<Channel> addSSLHandlerOnServerSide(ChannelHandlerContext ctx, ChannelPipeline pipeline)
            throws SSLException {
        LOGGER.debug("Adding a SSL handler on server side...");
        LOGGER.trace("Building a client SSL context for server side...");
        SslContext sslContextForServerSide = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        Configuration configuration = ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
        InetSocketAddress serverEndpoint = configuration.getServerEndpoint();
        SslHandler sslHandler = sslContextForServerSide.newHandler(ctx.alloc(), serverEndpoint.getHostString(),
                serverEndpoint.getPort());
        pipeline.addFirst("SSLHandler", sslHandler);
        Future<Channel> handshakeFuture = sslHandler.handshakeFuture();
        LOGGER.debug("SSL handler added SSL on server side");
        return handshakeFuture;
    }

}
