package eu.clarussecure.proxy.protocol.plugins.tcp;

import eu.clarussecure.proxy.spi.protocol.Configuration;
import io.netty.util.AttributeKey;

public interface TCPConstants {

    AttributeKey<Configuration> CONFIGURATION_KEY = AttributeKey.<Configuration>newInstance("CONFIGURATION_KEY");

    AttributeKey<TCPSession> SESSION_KEY = AttributeKey.<TCPSession>newInstance("SESSION_KEY");
}
