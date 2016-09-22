package eu.clarussecure.proxy.spi.protocol;

import java.util.List;

import eu.clarussecure.proxy.spi.DataOperation;

public interface ProtocolService {

    List<DataOperation> newDataOperation(DataOperation dataOperation);
}
