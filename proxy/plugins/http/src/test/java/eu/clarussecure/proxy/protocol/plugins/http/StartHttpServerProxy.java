package eu.clarussecure.proxy.protocol.plugins.http;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

public class StartHttpServerProxy implements Callable<Void> {

	@Override
	public Void call() throws Exception {

		HttpProtocol httpProtocol = new HttpProtocol();
		httpProtocol.getConfiguration().setListenPort(80);
		httpProtocol.getConfiguration().setServerAddress(InetAddress.getLocalHost());
		httpProtocol.getConfiguration()
				.setServerEndpoint(new InetSocketAddress(InetAddress.getByName("proxy2.akka.eu"), 9090));

		httpProtocol.getConfiguration().setProcessingMode(true, Operation.CREATE, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().setProcessingMode(true, Operation.READ, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().setProcessingMode(true, Operation.UPDATE, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().setProcessingMode(true, Operation.DELETE, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().setProcessingMode(false, Operation.CREATE, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().setProcessingMode(false, Operation.READ, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().setProcessingMode(false, Operation.UPDATE, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().setProcessingMode(false, Operation.DELETE, Mode.AS_IT_IS);
		httpProtocol.getConfiguration().register(new ProtocolServiceNoop());
		httpProtocol.start();
		Thread.sleep(500);
		httpProtocol.sync();
		return null;
	}

	public static void main(String[] args) throws Exception {
		new StartHttpServerProxy().call();
	}

}
