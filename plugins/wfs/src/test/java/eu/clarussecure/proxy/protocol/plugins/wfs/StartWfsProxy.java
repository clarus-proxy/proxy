package eu.clarussecure.proxy.protocol.plugins.wfs;

import eu.clarussecure.proxy.spi.protocol.Configuration;

import java.net.InetAddress;
import java.util.concurrent.Callable;

public class StartWfsProxy implements Callable<Void> {

    public Void call() throws Exception {

        String protocolName = "WFS";

        WfsProtocol protocol = new WfsProtocol();

        protocol.getConfiguration().setListenPort(4567);
        protocol.getConfiguration().setServerAddress(InetAddress.getByName("10.15.0.90"));

        Configuration configuration = protocol.getConfiguration();
        if (protocolName.equalsIgnoreCase(configuration.getProtocolName())) {
            //return protocol;
            System.out.println("wfs protocol");
        }

        protocol.start();

        return null;
    }

    public static void main(String[] args) throws Exception {
        new StartWfsProxy().call();
    }
}
