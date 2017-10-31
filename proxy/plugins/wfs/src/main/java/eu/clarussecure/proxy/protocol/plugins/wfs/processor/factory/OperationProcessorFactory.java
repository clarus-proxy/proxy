package eu.clarussecure.proxy.protocol.plugins.wfs.processor.factory;

import eu.clarussecure.proxy.protocol.plugins.wfs.model.Operation;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.exception.OperationNotSupportedException;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.OperationProcessor;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.TransactionProcessor;

public class OperationProcessorFactory {

    private static OperationProcessorFactory instance;

    public static synchronized OperationProcessorFactory getInstance() {
        if (instance == null) {
            instance = new OperationProcessorFactory();
        }
        return instance;
    }

    public OperationProcessor createOperationProcessor(Operation operation) throws OperationNotSupportedException {

        if (operation == null) {
            throw new OperationNotSupportedException("Unknown WFS Operation");
        }
        OperationProcessor operationProcessor = null;
        switch (operation) {
        case TRANSACTION:
            operationProcessor = new TransactionProcessor();
            break;
        // TODO Add other operation processors
        default:
            throw new OperationNotSupportedException("WFS Operation not supported: " + operation);
        }
        return operationProcessor;

    }

}
