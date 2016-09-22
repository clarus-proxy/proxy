package eu.clarussecure.proxy.spi.protocol;

import eu.clarussecure.proxy.spi.Capabilities;

public interface ProtocolCapabilities extends Capabilities {

    boolean isUserIdentificationRequired();

    boolean isUserAuthenticationSupported();

    boolean isUserSessionSupported();

    boolean isUserSessionSameAsTCPSession();

}
