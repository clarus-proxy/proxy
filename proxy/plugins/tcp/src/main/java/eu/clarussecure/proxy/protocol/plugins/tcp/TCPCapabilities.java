package eu.clarussecure.proxy.protocol.plugins.tcp;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

public class TCPCapabilities implements ProtocolCapabilities {

    private static final Set<Operation> DEFAULT_SUPPORTED_OPERATIONS = Operation.stream(Operation.values()).collect(Collectors.toSet());
    private static final Set<Mode> DEFAULT_SUPPORTED_PROCESSING_MODES = Collections.singleton(Mode.AS_IT_IS);
    private static final boolean DEFAULT_USER_IDENTIFICATION_REQUIRED = false;
    private static final boolean DEFAULT_USER_IDENTIFICATION_SUPPORTED = false;
    private static final boolean DEFAULT_USER_SESSION_SUPPORTED = true;
    private static final boolean DEFAULT_USER_SESSION_SAME_AS_TCPSESSION = true;

    @Override
    public Set<Operation> getSupportedCRUDOperations(boolean wholeDataset) {
        return DEFAULT_SUPPORTED_OPERATIONS;
    }

    @Override
    public Set<Mode> getSupportedProcessingModes(boolean wholeDataset, Operation operation) {
        return DEFAULT_SUPPORTED_PROCESSING_MODES;
    }

    @Override
    public boolean isUserIdentificationRequired() {
        return DEFAULT_USER_IDENTIFICATION_REQUIRED;
    }

    @Override
    public boolean isUserAuthenticationSupported() {
        return DEFAULT_USER_IDENTIFICATION_SUPPORTED;
    }

    @Override
    public boolean isUserSessionSupported() {
        return DEFAULT_USER_SESSION_SUPPORTED;
    }

    @Override
    public boolean isUserSessionSameAsTCPSession() {
        return DEFAULT_USER_SESSION_SAME_AS_TCPSESSION;
    }

}
