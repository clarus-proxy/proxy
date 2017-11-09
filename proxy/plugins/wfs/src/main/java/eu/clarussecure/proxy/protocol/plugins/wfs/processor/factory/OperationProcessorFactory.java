package eu.clarussecure.proxy.protocol.plugins.wfs.processor.factory;

import eu.clarussecure.proxy.protocol.plugins.wfs.model.WfsOperation;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.exception.OperationNotSupportedException;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.OperationProcessor;
import eu.clarussecure.proxy.protocol.plugins.wfs.processor.TransactionProcessor;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;

public class OperationProcessorFactory {

    private static OperationProcessorFactory instance;

    public static synchronized OperationProcessorFactory getInstance() {
        if (instance == null) {
            instance = new OperationProcessorFactory();
        }
        return instance;
    }

    public OperationProcessor createOperationProcessor(WfsOperation wfsOperation, XMLEventReader reader,
            XMLEventWriter writer) throws OperationNotSupportedException {

        if (wfsOperation == null) {
            throw new OperationNotSupportedException("Unknown WFS WfsOperation");
        }
        OperationProcessor operationProcessor = null;
        switch (wfsOperation) {
        case TRANSACTION:
            operationProcessor = new TransactionProcessor(reader, writer);
            break;
        // TODO Add other wfsOperation processors
        default:
            throw new OperationNotSupportedException("WFS WfsOperation not supported: " + wfsOperation);
        }
        return operationProcessor;

    }

}
