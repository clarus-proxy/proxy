package eu.clarussecure.proxy.protocol.plugins.wfs.parser.message;

import eu.clarussecure.proxy.protocol.plugins.wfs.parser.exception.WfsParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.Operation;
import eu.clarussecure.proxy.protocol.plugins.wfs.model.WfsParameter;
import eu.clarussecure.proxy.protocol.plugins.wfs.parser.WfsParserUtil;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Created on 04/07/2017.
 */
public class WfsGetRequest extends WfsRequest {

    protected Map<String, String> wfsParams;
    protected Operation wfsOperation;

    public WfsGetRequest(HttpVersion httpVersion, HttpMethod method, String uri) throws WfsParsingException {
        super(httpVersion, method, uri);
        setWfsParams();
    }

    public boolean hasMultipleParameters() {
        return (this.uri().indexOf("&") != -1);
    }

    public boolean isWfsRequest() {

        for (Map.Entry<String, String> entry : wfsParams.entrySet()) {
            if (entry.getKey().equals(WfsParameter.SERVICE.getParameter())) {
                return (entry.getValue().equalsIgnoreCase("WFS"));
            }
        }
        return false;
    }

    public void setWfsParams() throws WfsParsingException {
        try {
            wfsParams = WfsParserUtil.parseUrl(this);

        } catch (MalformedURLException e) {
            // handle the exception locally
            e.printStackTrace();

        } catch (URISyntaxException e) {
            // handle the exception locally
            e.printStackTrace();
        }
    }

    public Operation getWfsOperation() throws WfsParsingException {

        for (Map.Entry<String, String> entry : wfsParams.entrySet()) {
            if (entry.getKey().equals(WfsParameter.REQUEST.getParameter())) {
                return Operation.valueOfByName(entry.getValue());
            }
        }
        throw new WfsParsingException("No WFS operation found");
    }

    public Map<String, String> getWfsParams() {

        return wfsParams;
    }

}
