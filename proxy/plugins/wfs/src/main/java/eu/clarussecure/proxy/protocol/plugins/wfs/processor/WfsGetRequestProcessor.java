package eu.clarussecure.proxy.protocol.plugins.wfs.processor;

import eu.clarussecure.proxy.protocol.plugins.tcp.TCPConstants;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsGetRequest;
import eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec.WfsParameter;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.ModuleOperation;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.Configuration;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created on 23/06/2017.
 */
public class WfsGetRequestProcessor implements EventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    @Override
    public ProtocolService getProtocolService(ChannelHandlerContext ctx) {
        Configuration configuration = ctx.channel().attr(TCPConstants.CONFIGURATION_KEY).get();
        return configuration.getProtocolService();
    }

    /**
     * processGetCapabilities
     * @param ctx
     * @param object
     * @return
     */
    public HttpObject processGetCapabilities(ChannelHandlerContext ctx, HttpObject object) {

        return object;
    }

    /**
     * processDescribeFeatureType
     * @param ctx
     * @param object
     * @return
     */
    public HttpObject processDescribeFeatureType(ChannelHandlerContext ctx, HttpObject object) {

        return object;
    }

    /**
     * processGetFeature
     * @param ctx
     * @param request
     * @return
     */
    public HttpObject processGetFeature(ChannelHandlerContext ctx, WfsGetRequest request) {

        Operation operation = null;
        ProtocolService protocolService = getProtocolService(ctx);
        List<ByteBuf> parameterValues = null;

        boolean modifyRequests = true;
        /*TODO define if requests need to be modified (default yes) */

        // case of splitting
        if (modifyRequests) {

            ModuleOperation moduleOperation = extractGetFeatureOperation(ctx, request, parameterValues, null, null);

        }

        return request;

    }

    /**
     * extractGetFeatureOperation
     * @param ctx
     * @param request
     * @param parameterValues
     * @param dataOperation
     * @param operation
     * @return
     */
    private ModuleOperation extractGetFeatureOperation(ChannelHandlerContext ctx, WfsGetRequest request,
            List<ByteBuf> parameterValues, DataOperation dataOperation, Operation operation) {

        ModuleOperation moduleOperation = null;

        if (operation == null) {
            operation = Operation.READ;
        }

        List<String> storeIds = new ArrayList<>();
        List<String> layerIds = new ArrayList<>();
        List<String> dataIds = new ArrayList<>();

        Map<String, String> wfsParams = request.getWfsParams();

        wfsParams.forEach((key, value) -> {
            // extract store ids
            // extract layer ids
            if (key.equals(WfsParameter.TYPE_NAME.getParameter())) {
                String param = value.toString();
                String[] typeName = param.split(":");
                String storeId;
                String layerId;
                if (typeName.length > 1) {
                    storeId = typeName[0];
                    layerId = typeName[1];
                } else {
                    storeId = "";
                    layerId = typeName[0];
                }
            }

            // extract data ids
            if (key.equals(WfsParameter.PROPERTY_NAME.getParameter())) {
                String param = value.toString();
                String[] propertyNames = param.split(",");
            }
        });

        dataOperation = new DataOperation();
        dataOperation.setOperation(operation);

        // dataOperation.setDataIds();

        moduleOperation = dataOperation;

        return moduleOperation;

    }

}
