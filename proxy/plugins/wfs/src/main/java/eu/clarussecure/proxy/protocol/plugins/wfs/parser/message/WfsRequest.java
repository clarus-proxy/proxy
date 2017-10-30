package eu.clarussecure.proxy.protocol.plugins.wfs.parser.message;

import eu.clarussecure.proxy.protocol.plugins.wfs.exception.WfsParsingException;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

/**
 * Created on 26/06/2017.
 */
public class WfsRequest extends DefaultHttpRequest {

    public WfsRequest(HttpVersion httpVersion, HttpMethod method, String uri) throws WfsParsingException {

        super(httpVersion, method, uri);
    }

}
