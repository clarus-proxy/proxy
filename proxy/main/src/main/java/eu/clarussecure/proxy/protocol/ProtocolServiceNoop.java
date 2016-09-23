package eu.clarussecure.proxy.protocol;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;

public class ProtocolServiceNoop implements ProtocolService {

    @Override
    public List<DataOperation> newDataOperation(DataOperation dataOperation) {
        Objects.requireNonNull(dataOperation);
        return Collections.singletonList(dataOperation);
    }

}
