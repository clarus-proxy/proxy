package eu.clarussecure.proxy.spi.protocol;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;

public interface Configurable {

    String getProtocolName();

    int getDefaultListenPort();

    int getListenPort();

    void setListenPort(int listenPort);

    InetSocketAddress getServerEndpoint();

    Set<InetSocketAddress> getServerEndpoints();

    void setServerAddress(InetAddress serverAddress);

    void setServerEndpoint(InetSocketAddress serverEndpoint);

    void setServerAddresses(Set<InetAddress> serverAddresss);

    void setServerEndpoints(Set<InetSocketAddress> serverEndpoints);

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
