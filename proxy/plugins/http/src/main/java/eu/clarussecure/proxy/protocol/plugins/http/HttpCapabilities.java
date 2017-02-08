package eu.clarussecure.proxy.protocol.plugins.http;

import java.util.Map;
import java.util.Set;

import eu.clarussecure.proxy.spi.Capabilities;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

public class HttpCapabilities implements ProtocolCapabilities {

	private final Map<Operation, Set<Mode>> datasetCRUDOperations = Capabilities
			.toMap(new Enum<?>[][] { { Operation.CREATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.READ, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.UPDATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.DELETE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING } });

	private final Map<Operation, Set<Mode>> recordCRUDOperations = Capabilities
			.toMap(new Enum<?>[][] { { Operation.CREATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.READ, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING, Mode.ORCHESTRATION },
					{ Operation.UPDATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.DELETE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING } });

	@Override
	public Set<Operation> getSupportedCRUDOperations(boolean wholeDataset) {
		return wholeDataset ? datasetCRUDOperations.keySet() : recordCRUDOperations.keySet();
	}

	@Override
	public Set<Mode> getSupportedProcessingModes(boolean wholeDataset, Operation operation) {
		return wholeDataset ? datasetCRUDOperations.get(operation) : recordCRUDOperations.get(operation);
	}

	@Override
	public boolean isUserIdentificationRequired() {
		return true;
	}

	@Override
	public boolean isUserAuthenticationSupported() {
		return true;
	}

	@Override
	public boolean isUserSessionSupported() {
		return true;
	}

	@Override
	public boolean isUserSessionSameAsTCPSession() {
		return true;
	}

}
