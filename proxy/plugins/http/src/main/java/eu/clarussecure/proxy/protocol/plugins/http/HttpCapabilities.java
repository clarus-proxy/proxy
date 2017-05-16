/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http;

import java.util.Map;
import java.util.Set;

import eu.clarussecure.proxy.spi.Capabilities;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

// TODO: Auto-generated Javadoc
/**
 * The Class HttpCapabilities.
 */
public class HttpCapabilities implements ProtocolCapabilities {

	/** The dataset CRUD operations. */
	private final Map<Operation, Set<Mode>> datasetCRUDOperations = Capabilities
			.toMap(new Enum<?>[][] { { Operation.CREATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.READ, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.UPDATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.DELETE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING } });

	/** The record CRUD operations. */
	private final Map<Operation, Set<Mode>> recordCRUDOperations = Capabilities
			.toMap(new Enum<?>[][] { { Operation.CREATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.READ, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING, Mode.ORCHESTRATION },
					{ Operation.UPDATE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING },
					{ Operation.DELETE, Mode.AS_IT_IS, Mode.BUFFERING, Mode.STREAMING } });

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.Capabilities#getSupportedCRUDOperations(boolean)
	 */
	@Override
	public Set<Operation> getSupportedCRUDOperations(boolean wholeDataset) {
		return wholeDataset ? datasetCRUDOperations.keySet() : recordCRUDOperations.keySet();
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.Capabilities#getSupportedProcessingModes(boolean, eu.clarussecure.proxy.spi.Operation)
	 */
	@Override
	public Set<Mode> getSupportedProcessingModes(boolean wholeDataset, Operation operation) {
		return wholeDataset ? datasetCRUDOperations.get(operation) : recordCRUDOperations.get(operation);
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities#isUserIdentificationRequired()
	 */
	@Override
	public boolean isUserIdentificationRequired() {
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities#isUserAuthenticationSupported()
	 */
	@Override
	public boolean isUserAuthenticationSupported() {
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities#isUserSessionSupported()
	 */
	@Override
	public boolean isUserSessionSupported() {
		return true;
	}

	/* (non-Javadoc)
	 * @see eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities#isUserSessionSameAsTCPSession()
	 */
	@Override
	public boolean isUserSessionSameAsTCPSession() {
		return true;
	}

}
