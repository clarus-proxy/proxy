package eu.clarussecure.proxy;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
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
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;
import eu.clarussecure.proxy.spi.security.policy.SecurityPolicy;

public class Proxy {
    protected static final Logger LOGGER = LoggerFactory.getLogger(Proxy.class);

    private final String securityPolicyPath;
    private final List<String> serverAddresses;
    private final Integer maxFrameLen;
    private final Integer nbListenThreads;
    private final Integer nbSessionThreads;
    private final Integer nbParserThreads;
    private SecurityPolicy securityPolicy;
    private Protocol protocol;

    private Proxy(String securityPolicyPath, List<String> serverAddresses, Integer maxFrameLen, Integer nbListenThreads,
            Integer nbSessionThreads, Integer nbParserThreads) {
        this.securityPolicyPath = securityPolicyPath;
        this.serverAddresses = serverAddresses;
        this.maxFrameLen = maxFrameLen;
        this.nbListenThreads = nbListenThreads;
        this.nbSessionThreads = nbSessionThreads;
        this.nbParserThreads = nbParserThreads;
    }

    private void initialize() throws ParserConfigurationException, SAXException, IOException {
        LOGGER.trace("Loading security policy from file: {} ...", securityPolicyPath);
        // Load security policy
        securityPolicy = SecurityPolicy.load(new File(securityPolicyPath));
        LOGGER.debug("Security policy loaded from file: {}", securityPolicyPath);
        // Load protection module
        String protectionModuleName = securityPolicy.getProtectionModuleName();
        ProtectionModule protectionModule = null;
        if (protectionModuleName == null) {
            LOGGER.debug("No protection module");
        } else {
            LOGGER.trace("Loading protection module '{}' ...", protectionModuleName);
            protectionModule = ProtectionModuleLoader.getInstance().getProtectionModule(protectionModuleName);
            LOGGER.debug("Protection module '{}' loaded", protectionModuleName);
        }
        // Load protocol plugin
        String pluginName = securityPolicy.getProtocolPluginName();
        LOGGER.trace("Loading protocol plugin '{}' ...", pluginName);
        protocol = ProtocolLoader.getInstance().getProtocol(pluginName);
        LOGGER.debug("Protocol plugin '{}' loaded", pluginName);
        // Configure processing modes
        LOGGER.trace("Configuring processing modes...");
        ProtectionModuleCapabilities protectionModuleCapabilities = null;
        if (protectionModule != null) {
            protectionModuleCapabilities = protectionModule.getCapabilities();
        }
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
        // Configure the frame length
        if (maxFrameLen != null) {
            protocolConfiguration.setFramePartMaxLength(maxFrameLen);
        }
        // Configure the event executor groups (pools of threads)
        if (nbListenThreads != null) {
            protocolConfiguration.setNbListenThreads(nbListenThreads);
        }
        if (nbSessionThreads != null) {
            protocolConfiguration.setNbSessionThreads(nbSessionThreads);
        }
        if (nbParserThreads != null) {
            protocolConfiguration.setNbParserThreads(nbParserThreads);
        }
        // Initialize the protection module
        if (protectionModule != null) {
            LOGGER.trace("Initializing the protection module...");
            protectionModule.initialize(securityPolicy.getDocument(), securityPolicy.getDataIds());
            LOGGER.debug("Protection module initialized");
        }
        // Register the protocol service
        ProtocolService protocolService;
        if (protectionModule != null) {
            protocolService = new ProtocolServiceDelegate(protectionModule);
        } else {
            protocolService = new ProtocolServiceNoop();
        }
        protocolConfiguration.register(protocolService);
        LOGGER.debug("Protocol service registered");
        LOGGER.info("The CLARUS proxy is ready to intercept {} protocol and to protect data with {}",
                protocolConfiguration.getProtocolName(), protectionModuleName);
    }

    private void configureProcessingModes(boolean wholedataset,
            ProtectionModuleCapabilities protectionModuleCapabilities, ProtocolCapabilities protocolCapabilities,
            Configuration protocolConfiguration) {
        LOGGER.trace("Configuring processing modes for access to {}...", wholedataset ? "wholedataset" : "records");
        Set<Operation> protectionOps;
        if (protectionModuleCapabilities != null) {
            protectionOps = protectionModuleCapabilities.getSupportedCRUDOperations(wholedataset);
            LOGGER.trace("Protection module supports the following operations for access to {}: {}",
                    wholedataset ? "wholedataset" : "records", protectionOps);
        } else {
            protectionOps = Arrays.stream(Operation.values()).collect(Collectors.toSet());
        }
        Set<Operation> protocolOps = protocolCapabilities.getSupportedCRUDOperations(wholedataset);
        LOGGER.trace("Protocol plugin supports the following operations for access to {}: {}",
                wholedataset ? "wholedataset" : "records", protocolOps);
        Set<Operation> operations = EnumSet.copyOf(protocolOps);
        operations.retainAll(protectionOps);
        LOGGER.debug("Supported operations by both the protection module and the protocol plugin for access to {}: {}",
                wholedataset ? "wholedataset" : "records", operations);
        for (Operation operation : operations) {
            Mode protectionProcessingMode;
            if (protectionModuleCapabilities != null) {
                protectionProcessingMode = protectionModuleCapabilities.getPreferredProcessingMode(wholedataset,
                        operation, securityPolicy);
                LOGGER.trace(
                        "Preferred processing mode of the protection module (according to the security policy) for {} operation on {}: {}",
                        operation, wholedataset ? "wholedataset" : "records", protectionProcessingMode);
            } else {
                protectionProcessingMode = Mode.AS_IT_IS;
            }
            Set<Mode> protocolProcessingModes = protocolCapabilities.getSupportedProcessingModes(wholedataset,
                    operation);
            LOGGER.trace("Supported processing modes by the protocol module for {} operation on {}: {}", operation,
                    wholedataset ? "wholedataset" : "records", protocolProcessingModes);
            Set<Mode> processingModes = EnumSet.copyOf(protocolProcessingModes);
            processingModes.retainAll(Collections.singleton(protectionProcessingMode));
            Mode processingMode = processingModes.isEmpty() ? null : processingModes.iterator().next();
            protocolConfiguration.setProcessingMode(wholedataset, operation, processingMode);
            LOGGER.debug("Processing mode to use for {} operation on {}: {}", operation,
                    wholedataset ? "wholedataset" : "records", processingMode);
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
        Integer maxFrameLen = null;
        Integer nbListenThreads = null;
        Integer nbSessionThreads = null;
        Integer nbParserThreads = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("-sp".equals(arg) || "--security-policy".equals(arg)) {
                if (++i < args.length) {
                    securityPolicyPath = args[i];
                }
            } else if ("-mf".equals(arg) || "--max-frame-len".equals(arg)) {
                if (++i < args.length) {
                    maxFrameLen = Integer.parseInt(args[i]);
                    if (maxFrameLen <= 0) {
                        System.out.println("Maximum frame length must be a positive number");
                        usage();
                        return;
                    }
                }
            } else if ("-lt".equals(arg) || "--nb-listen-threads".equals(arg)) {
                if (++i < args.length) {
                    if ("cores".equals(args[i])) {
                        nbListenThreads = Runtime.getRuntime().availableProcessors();
                    } else {
                        nbListenThreads = Integer.parseInt(args[i]);
                    }
                    if (nbListenThreads <= 0) {
                        System.out.println(
                                "Number of listen threads must be a positive number or the special value 'cores' (number of cores)");
                        usage();
                        return;
                    }
                }
            } else if ("-st".equals(arg) || "--nb-session-threads".equals(arg)) {
                if (++i < args.length) {
                    if ("cores".equals(args[i])) {
                        nbSessionThreads = Runtime.getRuntime().availableProcessors();
                    } else {
                        nbSessionThreads = Integer.parseInt(args[i]);
                    }
                    if (nbSessionThreads <= 0) {
                        System.out.println(
                                "Number of session threads must be a positive number or the special value 'cores' (number of cores)");
                        usage();
                        return;
                    }
                }
            } else if ("-pt".equals(arg) || "--nb-parser-threads".equals(arg)) {
                if (++i < args.length) {
                    if ("cores".equals(args[i])) {
                        nbParserThreads = Runtime.getRuntime().availableProcessors();
                    } else {
                        nbParserThreads = Integer.parseInt(args[i]);
                    }
                    if (nbParserThreads < 0) {
                        System.out.println(
                                "Number of parser threads must be a positive number or 0 or the special value 'cores' (number of cores)");
                        usage();
                        return;
                    }
                }
            } else {
                serverAddresses.add(arg);
            }
        }
        if (securityPolicyPath == null || serverAddresses.isEmpty()) {
            if (securityPolicyPath == null) {
                System.out.println("The security policy is mandatory");
            }
            if (serverAddresses.isEmpty()) {
                System.out.println("At least one server address is mandatory");
            }
            usage();
            return;
        }
        Proxy proxy = new Proxy(securityPolicyPath, serverAddresses, maxFrameLen, nbListenThreads, nbSessionThreads,
                nbParserThreads);
        proxy.initialize();
        proxy.start();
    }

    private static void usage() {
        System.out.println(
                "usage: java -Djava.ext.dirs=<CLARUS_EXT_DIRS> [PROTOCOL OPTIONS] -jar proxy-1.0-SNAPSHOT.jar [OPTION]... [SERVER_ADDRESS]...");
        System.out.println("CLARUS extensions:");
        System.out.println(
                "  <CLARUS_EXT_DIRS>        list the extensions directories that contain protection modules and protocol plugins");
        System.out.println("Security policy options:");
        System.out.println(" -sp, --security-policy <PATH>");
        System.out.println("                           the security policy to apply");
        System.out.println("Resource consumption options:");
        System.out.println(" [-mf, --max-frame-len <MAX_FRAME_LENGTH>]");
        System.out.println("                           maximum frame length to process");
        System.out.println(" [-lt, --nb-listen-threads <NB_LISTEN_THREADS>]");
        System.out.println(
                "                           number of listen threads (default: 1). Must be a positive number or the special value 'cores' (number of cores)");
        System.out.println(" [-st, --nb-session-threads <NB_SESSION_THREADS>]");
        System.out.println(
                "                           number of session threads (default: number of cores). Must be a positive number or the special value 'cores' (number of cores)");
        System.out.println(" [-pt, --nb-parser-threads <NB_PARSER_THREADS>]");
        System.out.println(
                "                           number of parser threads (default: 0). Must be a positive number or 0 or the special value 'cores' (number of cores)");
        System.out.println("Protocol options:");
        System.out.println(" [-D<OPTION_NAME>=<OPTION_VALUE>]");
        System.out.println(
                "                           define options specific to the protocol plugin. Muliple options can be specified");
        System.out.println("Server addresses:");
        System.out.println(
                " <HOSTNAME>[:<PORT>]       server host and optional port. Muliple server addresses can be specified");
    }
}
