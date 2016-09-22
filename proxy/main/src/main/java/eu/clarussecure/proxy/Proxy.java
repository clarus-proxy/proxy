package eu.clarussecure.proxy;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import eu.clarussecure.proxy.protection.ProtectionModuleLoader;
import eu.clarussecure.proxy.protocol.ProtocolLoader;
import eu.clarussecure.proxy.protocol.ProtocolServiceDelegate;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protection.ProtectionModule;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.Protocol;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;
import eu.clarussecure.proxy.spi.security.policy.SecurityPolicy;

public class Proxy {
    protected static final Logger LOGGER = LoggerFactory.getLogger(Proxy.class);

    String securityPolicyPath;
    List<String> serverAddresses;
    private SecurityPolicy securityPolicy;
    private Protocol protocol;

    private Proxy(String securityPolicyPath, List<String> serverAddresses) {
        this.securityPolicyPath = securityPolicyPath;
        this.serverAddresses = serverAddresses;
    }

    private void initialize() throws ParserConfigurationException, SAXException, IOException {
        LOGGER.trace("Loading security policy from file: {} ...", securityPolicyPath);
        // Load security policy
        securityPolicy = SecurityPolicy.load(new File(securityPolicyPath));
        LOGGER.debug("Security policy loaded from file: {}", securityPolicyPath);
        // Load protection module
        String protectionModuleName = securityPolicy.getProtectionModuleName();
        LOGGER.trace("Loading protection module '{}' ...", protectionModuleName);
        ProtectionModule protectionModule = ProtectionModuleLoader.getInstance().getProtectionModule(protectionModuleName);
        LOGGER.debug("Protection module '{}' loaded", protectionModuleName);
        // Load protocol plugin
        String pluginName = securityPolicy.getProtocolPluginName();
        LOGGER.trace("Loading protocol plugin '{}' ...", pluginName);
        protocol = ProtocolLoader.getInstance().getProtocol(pluginName);
        LOGGER.debug("Protocol plugin '{}' loaded", pluginName);
        // Configure processing modes
        LOGGER.trace("Configuring processing modes...");
        ProtectionModuleCapabilities protectionModuleCapabilities = protectionModule.getCapabilities();
        ProtocolCapabilities protocolCapabilities = protocol.getCapabilities();
        Configuration protocolConfiguration = protocol.getConfiguration();
        // Configure dataset processing mode
        configureProcessingModes(true, protectionModuleCapabilities, protocolCapabilities, protocolConfiguration);
        // Configure records processing mode
        configureProcessingModes(false, protectionModuleCapabilities, protocolCapabilities, protocolConfiguration);
        LOGGER.debug("Processing modes configured");
        LOGGER.trace("Configuring internal and external endpoints...");
        // Configure listen port
        Integer listenPort = securityPolicy.getProtocolListenPort();
        if (listenPort != null) {
            protocolConfiguration.setListenPort(securityPolicy.getProtocolListenPort());
        }
        // Configure server addresses
        Set<InetSocketAddress> serverEndpoints = serverAddresses.stream().map(serverAddress -> {
            String[] tokens = serverAddress.split(":");
            String hostname = tokens[0];
            int port = tokens.length == 1 ? protocolConfiguration.getListenPort() : Integer.parseInt(tokens[1]);
            return new InetSocketAddress(hostname, port);
        }).collect(Collectors.toSet());
        protocolConfiguration.setServerEndpoints(serverEndpoints);
        LOGGER.debug("Internal and external endpoints configured");
        // Initialize the protection module
        LOGGER.trace("Initializing the protection module...");
        protectionModule.initialize(securityPolicy.getDocument());
        LOGGER.debug("Protection module initialized");
        // Register the protocol service
        protocolConfiguration.register(new ProtocolServiceDelegate(protectionModule));
        LOGGER.debug("Protocol service registered");
        LOGGER.info("The CLARUS proxy is ready to intercept {} protocol and to protect data with {}", protocolConfiguration.getProtocolName(), protectionModuleName);
    }

    private void configureProcessingModes(boolean wholedataset, ProtectionModuleCapabilities protectionModuleCapabilities,
            ProtocolCapabilities protocolCapabilities, Configuration protocolConfiguration) {
        LOGGER.trace("Configuring processing modes for access to {}...", wholedataset ? "wholedataset" : "records");
        Set<Operation> protectionOps = protectionModuleCapabilities.getSupportedCRUDOperations(wholedataset);
        LOGGER.trace("Protection module supports the following operations for access to {}: {}", wholedataset ? "wholedataset" : "records", protectionOps);
        Set<Operation> protocolOps = protocolCapabilities.getSupportedCRUDOperations(wholedataset);
        LOGGER.trace("Protocol plugin supports the following operations for access to {}: {}", wholedataset ? "wholedataset" : "records", protocolOps);
        Set<Operation> operations = EnumSet.copyOf(protocolOps);
        operations.retainAll(protectionOps);
        LOGGER.debug("Supported operations by both the protection module and the protocol plugin for access to {}: {}", wholedataset ? "wholedataset" : "records", operations);
        for (Operation operation : operations) {
            Mode protectionProcessingMode = protectionModuleCapabilities.getPreferredProcessingMode(wholedataset, operation, securityPolicy);
            LOGGER.trace("Preferred processing mode of the protection module (according to the security policy) for {} operation on {}: {}", operation, wholedataset ? "wholedataset" : "records", protectionProcessingMode);
            Set<Mode> protocolProcessingModes = protocolCapabilities.getSupportedProcessingModes(wholedataset, operation);
            LOGGER.trace("Supported processing modes by the protocol module for {} operation on {}: {}", operation, wholedataset ? "wholedataset" : "records", protocolProcessingModes);
            Set<Mode> processingModes = EnumSet.copyOf(protocolProcessingModes);
            processingModes.retainAll(Collections.singleton(protectionProcessingMode));
            Mode processingMode = processingModes.isEmpty() ? null : processingModes.iterator().next();
            protocolConfiguration.setProcessingMode(wholedataset, operation, processingMode);
            LOGGER.debug("Processing mode to use for {} operation on {}: {}", operation, wholedataset ? "wholedataset" : "records", processingMode);
        }
    }

    private void start() {
        protocol.start();
        protocol.sync();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return;
        }
        String securityPolicyPath = null;
        List<String> serverAddresses = new ArrayList<>();
        for (int i = 0; i < args.length; i ++) {
            String arg = args[i];
            if ("-sp".equals(arg) || "--security-policy".equals(arg)) {
                if (++i < args.length) {
                    securityPolicyPath = args[i];
                }
            } else {
                serverAddresses.add(arg);
            }
        }
        if (securityPolicyPath == null || serverAddresses.isEmpty()) {
            usage();
            return;
        }
        Proxy proxy = new Proxy(securityPolicyPath, serverAddresses);
        proxy.initialize();
        proxy.start();
    }

    private static void usage() {
        System.out.println("usage: java -Djava.ext.dirs=<CLARUS_EXT_DIRS> -jar proxy-1.0-SNAPSHOT.jar [OPTION]... [SERVER_ADDRESS]...");
        System.out.println("CLARUS extensions:");
        System.out.println("  <CLARUS_EXT_DIRS>            list the extensions directories that contain protection modules and protocol plugins");
        System.out.println("Security policy options:");
        System.out.println(" -sp, --security-policy <PATH> security policy to apply");
        System.out.println("Server addresses:");
        System.out.println(" <HOSTNAME>[:<PORT>]           server host and optional port. Muliple server addresses can be specified");
    }
}
