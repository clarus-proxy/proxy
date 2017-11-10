package eu.clarussecure.proxy.protection.modules.encryption;

import java.util.Map;
import java.util.Set;

import eu.clarussecure.proxy.spi.Capabilities;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protection.ProtectionModuleCapabilities;
import eu.clarussecure.proxy.spi.security.policy.SecurityPolicy;

public class EncryptionCapabilities implements ProtectionModuleCapabilities {

    private final Map<Operation, Set<Mode>> datasetCRUDOperations = Capabilities
            .toMap(new Enum<?>[][] { { Operation.CREATE, Mode.STREAMING }, { Operation.READ, Mode.STREAMING },
                    { Operation.UPDATE, Mode.STREAMING }, { Operation.DELETE, Mode.STREAMING } });

    private final Map<Operation, Set<Mode>> recordCRUDOperations = Capabilities
            .toMap(new Enum<?>[][] { { Operation.CREATE, Mode.STREAMING }, { Operation.READ, Mode.STREAMING },
                    { Operation.UPDATE, Mode.STREAMING }, { Operation.DELETE, Mode.STREAMING } });

    @Override
    public Set<Operation> getSupportedCRUDOperations(boolean wholeDataset) {
        return wholeDataset ? datasetCRUDOperations.keySet() : recordCRUDOperations.keySet();
    }

    @Override
    public Set<Mode> getSupportedProcessingModes(boolean wholeDataset, Operation operation) {
        return wholeDataset ? datasetCRUDOperations.get(operation) : recordCRUDOperations.get(operation);
    }

    @Override
    public Mode getPreferredProcessingMode(boolean wholedataset, Operation operation, SecurityPolicy securityPolicy) {
        Set<Mode> modes = getSupportedProcessingModes(wholedataset, operation);
        if (modes.size() <= 1) {
            return modes.isEmpty() ? null : modes.iterator().next();
        }
        return null;
    }
}
