/*
 * 
 */
package eu.clarussecure.proxy.protocol.plugins.http;

import java.net.InetAddress;
import java.util.concurrent.Callable;

import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolServiceNoop;

// TODO: Auto-generated Javadoc
/**
 * The Class StartHttpServerProxy.
 */
public class StartHttpServerProxy implements Callable<Void> {

	/* (non-Javadoc)
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Void call() throws Exception {

		// System.setProperty("tcp.ssl.certificate.file", new File("C:\\Program
		// Files\\Java\\jre1.8.0_121\\lib\\security\\jssecacerts.pem").getPath());
		// System.setProperty("tcp.ssl.private.key.file", new File("C:\\Program
		// Files\\Java\\jre1.8.0_121\\lib\\security\\privateKey.key").getPath());
				
		HttpProtocol httpProtocol = new HttpProtocol();
		httpProtocol.getConfiguration().setListenPort(9090);
		httpProtocol.getConfiguration().setServerAddress(InetAddress.getLocalHost());
		//httpProtocol.getConfiguration().setServerEndpoint(new InetSocketAddress("proxy10.akka.eu", 9090));
		
		
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

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		new StartHttpServerProxy().call();
	}

}
