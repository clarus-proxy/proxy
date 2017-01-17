package eu.clarussecure.proxy.spi.protocol;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;

public class ProtocolServiceNoop implements ProtocolService {

    @Override
    public MetadataOperation newMetadataOperation(MetadataOperation metadataOperation) {
        Objects.requireNonNull(metadataOperation);
        return metadataOperation;
    }

    @Override
    public List<DataOperation> newDataOperation(DataOperation dataOperation) {
        Objects.requireNonNull(dataOperation);
        return Collections.singletonList(dataOperation);
    }

    @Override
    public CString newUserIdentification(CString user) {
        return user;
    }

    @Override
    public CString[] userAuthentication(CString user, CString password) {
        return new CString[] { user, password };
    }
}
