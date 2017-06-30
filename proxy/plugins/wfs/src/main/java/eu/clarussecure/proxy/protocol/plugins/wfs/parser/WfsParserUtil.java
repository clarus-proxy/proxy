package eu.clarussecure.proxy.protocol.plugins.wfs.parser;

import eu.clarussecure.proxy.protocol.plugins.wfs.exception.WfsParsingException;
import io.netty.handler.codec.http.HttpRequest;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 23/06/2017.
 */
public class WfsParserUtil {

    public static Map<String, String> parseUrl(HttpRequest request)
            throws MalformedURLException, URISyntaxException, WfsParsingException {

        URI uri = new URI(request.uri());
        String query = uri.getQuery();

        if (query == null) {
            throw new WfsParsingException("no query in URI");
        }

        Map<String, String> map = new HashMap<String, String>();

        if (query.indexOf("&") == -1) {
            throw new WfsParsingException("no parameters");

        } else {

            Arrays.stream(query.split("&")).forEach(param -> {
                int idx = param.indexOf("=");
                map.put(param.substring(0, idx), param.substring(idx + 1));
            });
        }

        return map;

    }

}
