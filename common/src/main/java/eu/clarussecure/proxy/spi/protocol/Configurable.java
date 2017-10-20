package eu.clarussecure.proxy.spi.protocol;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.security.policy.SecurityPolicy;

public interface Configurable {

    String getProtocolName();

    int getDefaultProtocolPort();

    int getListenPort();

    void setListenPort(int listenPort);

    default InetSocketAddress getServerEndpoint() {
        return getServerEndpoint(0);
    }

    InetSocketAddress getServerEndpoint(int index);

    List<InetSocketAddress> getServerEndpoints();

    void setServerAddress(InetAddress serverAddress);

    void setServerEndpoint(InetSocketAddress serverEndpoint);

    void setServerAddresses(List<InetAddress> serverAddresses);

    void setServerAddresses(InetAddress... serverAddresses);

    void setServerEndpoints(List<InetSocketAddress> serverEndpoints);

    void setServerEndpoints(InetSocketAddress... serverEndpoints);

    Map<String, String> getParameters();

    void setParameters(Map<String, String> parameters);

    default void setParameters(SecurityPolicy securityPolicy) {
        setParameters(securityPolicy.getProtocolParameters());
    }

    int getNbListenThreads();

    void setNbListenThreads(int nThreads);

    int getNbSessionThreads();

    void setNbSessionThreads(int nThreads);

    int getNbParserThreads();

    void setNbParserThreads(int nThreads);

    int getFramePartMaxLength();

    void setFramePartMaxLength(int maxlen);

    void registerDataTypes();

    Mode getProcessingMode(boolean wholeDataset, Operation operation);

    void setProcessingMode(boolean wholeDataset, Operation operation, Mode mode);

    Set<String> getComputationCommands();

    void setComputationCommands(Set<String> commands);

    ProtocolService getProtocolService();

    void register(ProtocolService protocolService);

}
