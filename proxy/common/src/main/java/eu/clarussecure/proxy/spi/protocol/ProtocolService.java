package eu.clarussecure.proxy.spi.protocol;

import java.util.List;

import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;

public interface ProtocolService {

    MetadataOperation newMetadataOperation(MetadataOperation metadataOperation);

    List<DataOperation> newDataOperation(DataOperation dataOperation);

    CString newUserIdentification(CString user);

}
