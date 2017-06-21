package eu.clarussecure.proxy.spi.protocol;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.InboundDataOperation;
import eu.clarussecure.proxy.spi.MetadataOperation;
import eu.clarussecure.proxy.spi.OutboundDataOperation;

public class ProtocolServiceNoop implements ProtocolService {

    @Override
    public MetadataOperation newMetadataOperation(MetadataOperation metadataOperation) {
        Objects.requireNonNull(metadataOperation);
        return metadataOperation;
    }

    @Override
    public List<OutboundDataOperation> newOutboundDataOperation(OutboundDataOperation outboundDataOperation) {
        Objects.requireNonNull(outboundDataOperation);
        if (outboundDataOperation.getInvolvedCSPs() != null && outboundDataOperation.getInvolvedCSPs().size() > 1) {
            throw new IllegalArgumentException("cannot support more than one CSP");
        }
        List<OutboundDataOperation> newOutboundDataOperations = Collections.singletonList(outboundDataOperation);

        return newOutboundDataOperations;
    }

    @Override
    public InboundDataOperation newInboundDataOperation(List<InboundDataOperation> inboundDataOperations) {
        Objects.requireNonNull(inboundDataOperations);
        if (inboundDataOperations.size() > 1) {
            throw new IllegalArgumentException("cannot support more than one CSP");
        }
        InboundDataOperation inboundDataOperation = inboundDataOperations.get(0);
        if (inboundDataOperation.getInvolvedCSPs() != null && inboundDataOperation.getInvolvedCSPs().size() > 1) {
            throw new IllegalArgumentException("cannot support more than one CSP");
        }

        return inboundDataOperation;
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
