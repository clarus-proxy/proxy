/*
 *
 */
package eu.clarussecure.proxy.protocol.plugins.wfs;

import eu.clarussecure.proxy.protocol.plugins.http.HttpConfiguration;
import eu.clarussecure.proxy.spi.protocol.ProtocolCapabilities;

public class WfsConfiguration extends HttpConfiguration {

    /**
     * Instantiates a new WFS configuration.
     *
     * @param capabilities the capabilities
     */
    public WfsConfiguration(ProtocolCapabilities capabilities) {
        super(capabilities);
    }

    @Override
    public String getProtocolName() {

        return "WFS";
    }

    @Override
    public int getDefaultProtocolPort() {

        return 8080;
    }

}
