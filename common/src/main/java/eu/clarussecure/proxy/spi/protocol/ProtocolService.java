package eu.clarussecure.proxy.spi.protocol;

import java.util.List;

import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.InboundDataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;
import eu.clarussecure.proxy.spi.OutboundDataOperation;

public interface ProtocolService {

    MetadataOperation newMetadataOperation(MetadataOperation metadataOperation);

    List<OutboundDataOperation> newOutboundDataOperation(OutboundDataOperation outboundDataOperation);

    InboundDataOperation newInboundDataOperation(List<InboundDataOperation> inboundDataOperations);

    CString newUserIdentification(CString user);

    CString[] userAuthentication(CString user, CString password);
}
