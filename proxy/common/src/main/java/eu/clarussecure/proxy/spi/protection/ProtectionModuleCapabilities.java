package eu.clarussecure.proxy.spi.protection;

import eu.clarussecure.proxy.spi.Capabilities;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.security.policy.SecurityPolicy;

public interface ProtectionModuleCapabilities extends Capabilities {

    Mode getPreferredProcessingMode(boolean wholedataset, Operation operation, SecurityPolicy securityPolicy);

}
