package eu.clarussecure.proxy.spi.protocol;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;

public abstract class Configuration implements Configurable {

    public static final int DEFAULT_NB_OF_LISTEN_THREADS = 1;

    public static final int DEFAULT_NB_OF_SESSION_THREADS = Runtime.getRuntime().availableProcessors();

    public static final int DEFAULT_NB_OF_PARSER_THREADS = 0;

    public static final int DEFAULT_FRAME_PART_MAX_LENGTH = Integer.MAX_VALUE;

    protected final ProtocolCapabilities capabilities;

    protected int listenPort;

    protected List<InetSocketAddress> serverEndpoints;

    protected Map<String, String> parameters;

    protected int nbListenThreads = DEFAULT_NB_OF_LISTEN_THREADS;

    protected int nbSessionThreads = DEFAULT_NB_OF_SESSION_THREADS;

    protected int nbParserThreads = DEFAULT_NB_OF_PARSER_THREADS;

    protected int framePartMaxLength = DEFAULT_FRAME_PART_MAX_LENGTH;

    protected Map<Operation, Mode> datasetProcessingModes = new HashMap<Operation, Mode>();
    protected Map<Operation, Mode> recordProcessingModes = new HashMap<Operation, Mode>();

    protected Set<String> commands = null;

    protected ProtocolService protocolService = null;

    public Configuration(ProtocolCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public int getListenPort() {
        return listenPort != 0 ? listenPort : getDefaultProtocolPort();
    }

    @Override
    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    @Override
    public InetSocketAddress getServerEndpoint(int index) {
        if (serverEndpoints != null) {
            if (index >= serverEndpoints.size()) {
                throw new IllegalArgumentException(
                        String.format("index (%d) >= number of endpoints (%d)", index, serverEndpoints.size()));
            }
            return serverEndpoints.get(index);
        }
        return null;
    }

    @Override
    public List<InetSocketAddress> getServerEndpoints() {
        return serverEndpoints;
    }

    @Override
    public void setServerAddress(InetAddress serverAddress) {
        serverEndpoints = Collections.singletonList(new InetSocketAddress(serverAddress, getDefaultProtocolPort()));
    }

    @Override
    public void setServerEndpoint(InetSocketAddress serverEndpoint) {
        serverEndpoints = Collections.singletonList(serverEndpoint);
    }

    @Override
    public void setServerAddresses(List<InetAddress> serverAddresses) {
        serverEndpoints = serverAddresses.stream()
                .map(serverAddress -> new InetSocketAddress(serverAddress, getDefaultProtocolPort()))
                .collect(Collectors.toList());
    }

    @Override
    public void setServerAddresses(InetAddress... serverAddresses) {
        serverEndpoints = Stream.of(serverAddresses)
                .map(serverAddress -> new InetSocketAddress(serverAddress, getDefaultProtocolPort()))
                .collect(Collectors.toList());
    }

    @Override
    public void setServerEndpoints(List<InetSocketAddress> serverEndpoints) {
        this.serverEndpoints = serverEndpoints;
    }

    @Override
    public void setServerEndpoints(InetSocketAddress... serverEndpoints) {
        this.serverEndpoints = Stream.of(serverEndpoints).collect(Collectors.toList());
    }

    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public int getNbListenThreads() {
        return nbListenThreads;
    }

    @Override
    public void setNbListenThreads(int nThreads) {
        if (nThreads <= 0) {
            throw new IllegalArgumentException("nThreads must be positive");
        }
        this.nbListenThreads = nThreads;
    }

    @Override
    public int getNbSessionThreads() {
        return nbSessionThreads;
    }

    @Override
    public void setNbSessionThreads(int nThreads) {
        if (nThreads <= 0) {
            throw new IllegalArgumentException("nThreads must be positive");
        }
        this.nbSessionThreads = nThreads;
    }

    @Override
    public int getNbParserThreads() {
        return nbParserThreads;
    }

    @Override
    public void setNbParserThreads(int nThreads) {
        if (nThreads < 0) {
            throw new IllegalArgumentException("nThreads must be positive or 0");
        }
        this.nbParserThreads = nThreads;
    }

    @Override
    public int getFramePartMaxLength() {
        return framePartMaxLength;
    }

    @Override
    public void setFramePartMaxLength(int maxlen) {
        this.framePartMaxLength = maxlen;
    }

    @Override
    public void registerDataTypes() {
    }

    @Override
    public Mode getProcessingMode(boolean wholeDataset, Operation operation) {
        if (!capabilities.getSupportedCRUDOperations(wholeDataset).contains(operation)) {
            throw new IllegalArgumentException(String.format("Operation '%s' is not supported on %s", operation,
                    wholeDataset ? "dataset" : "record"));
        }
        return wholeDataset ? datasetProcessingModes.get(operation) : recordProcessingModes.get(operation);
    }

    @Override
    public void setProcessingMode(boolean wholeDataset, Operation operation, Mode mode) {
        if (!capabilities.getSupportedCRUDOperations(wholeDataset).contains(operation)) {
            throw new IllegalArgumentException(String.format("Operation '%s' is not supported on %s", operation,
                    wholeDataset ? "dataset" : "record"));
        }
        if (mode != null && !capabilities.getSupportedProcessingModes(wholeDataset, operation).contains(mode)) {
            throw new IllegalArgumentException(
                    String.format("Processing mode '%s' is not supported for operation '%s' on %s", mode, operation,
                            wholeDataset ? "dataset" : "record"));
        }
        if (wholeDataset) {
            datasetProcessingModes.put(operation, mode);
        } else {
            recordProcessingModes.put(operation, mode);
        }
    }

    @Override
    public Set<String> getComputationCommands() {
        return commands;
    }

    @Override
    public void setComputationCommands(Set<String> commands) {
        if (!capabilities.getSupportedCRUDOperations(true).contains(Operation.READ)
                && !capabilities.getSupportedCRUDOperations(false).contains(Operation.READ)) {
            throw new IllegalArgumentException("Operation 'READ' is not supported on dataset and on record");
        }
        if (!capabilities.getSupportedProcessingModes(true, Operation.READ).contains(Mode.ORCHESTRATION)
                && !capabilities.getSupportedProcessingModes(false, Operation.READ).contains(Mode.ORCHESTRATION)) {
            throw new IllegalArgumentException(
                    "Processing mode 'ORCHESTRATION' is not supported for operation 'READ' on dataset and on record");
        }
        this.commands = commands;
    }

    @Override
    public ProtocolService getProtocolService() {
        return protocolService;
    }

    @Override
    public void register(ProtocolService protocolService) {
        this.protocolService = protocolService;
    }

}
