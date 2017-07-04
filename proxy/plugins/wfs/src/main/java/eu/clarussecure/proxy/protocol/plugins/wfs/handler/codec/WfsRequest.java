package eu.clarussecure.proxy.protocol.plugins.wfs.handler.codec;

import eu.clarussecure.proxy.protocol.plugins.wfs.exception.WfsParsingException;
import eu.clarussecure.proxy.protocol.plugins.wfs.parser.WfsParserUtil;
import io.netty.handler.codec.http.*;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Created on 26/06/2017.
 */
public class WfsRequest extends DefaultHttpRequest {

    protected Map<String, String> wfsParams;
    protected WfsOperation wfsOperation;

    public WfsRequest(HttpVersion httpVersion, HttpMethod method, String uri) throws WfsParsingException {

        super(httpVersion, method, uri);

        setWfsParams();

    }

    public boolean isGetRequest() {
        return this.method().equals(HttpMethod.GET);
    }

    public boolean isPostRequest() {
        return this.method().equals(HttpMethod.POST);
    }

    public boolean isWfsRequest() {

        for (Map.Entry<String, String> entry : wfsParams.entrySet()) {
            if (entry.getKey().equals(WfsParameter.SERVICE.getParameter())) {
                return (entry.getValue().equalsIgnoreCase("WFS"));
            }
        }
        return false;
    }

    public boolean isWfsGetRequest() {
        return isWfsRequest() && isGetRequest();
    }

    public boolean isWfsPostRequest() {
        return isWfsRequest() && isPostRequest();
    }

    public boolean hasMultipleParameters() {
        return (this.uri().indexOf("&") != -1);
    }

    /**
     * getWfsOperation
     * @return
     */
    public WfsOperation getWfsOperation() throws WfsParsingException {

        for (Map.Entry<String, String> entry : wfsParams.entrySet()) {
            if (entry.getKey().equals(WfsParameter.REQUEST.getParameter())) {
                return WfsOperation.valueOfByName(entry.getValue());
            }
        }

        throw new WfsParsingException("No WFS operation found");
    }

    /**
     * setWfsParams
     *
     */
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

    /**
     * getWfsParams
     * @return Map
     */
    public Map<String, String> getWfsParams() {

        return wfsParams;
    }

}
