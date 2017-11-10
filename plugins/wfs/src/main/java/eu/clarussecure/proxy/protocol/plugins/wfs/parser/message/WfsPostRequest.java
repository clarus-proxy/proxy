package eu.clarussecure.proxy.protocol.plugins.wfs.parser.message;

import eu.clarussecure.proxy.protocol.plugins.wfs.parser.exception.WfsParsingException;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

/**
 * Created on 04/07/2017.
 */
public class WfsPostRequest extends WfsRequest {

    public WfsPostRequest(HttpVersion httpVersion, HttpMethod method, String uri) throws WfsParsingException {
        super(httpVersion, method, uri);
    }

}
