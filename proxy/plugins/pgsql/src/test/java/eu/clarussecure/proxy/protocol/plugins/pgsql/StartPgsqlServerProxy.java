package eu.clarussecure.proxy.protocol.plugins.pgsql;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.DataOperation;
import eu.clarussecure.proxy.spi.Mode;
import eu.clarussecure.proxy.spi.Operation;
import eu.clarussecure.proxy.spi.protocol.ProtocolService;

public class StartPgsqlServerProxy implements Callable<Void> {


    public Void call() throws Exception {
        PgsqlProtocol pgsqlProtocol = new PgsqlProtocol();
        pgsqlProtocol.getConfiguration().setServerAddress(InetAddress.getByName("10.15.0.89"));
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.CREATE, Mode.BUFFERING);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.READ, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.UPDATE, null);
        pgsqlProtocol.getConfiguration().setProcessingMode(true, Operation.DELETE, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.CREATE, null);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.READ, Mode.AS_IT_IS);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.UPDATE, null);
        pgsqlProtocol.getConfiguration().setProcessingMode(false, Operation.DELETE, null);
        pgsqlProtocol.getConfiguration().register( new ProtocolService() {
			
			@Override
			public CString newUserIdentification(CString user) {
				return user;
			}
			
			@Override
			public List<DataOperation> newDataOperation(DataOperation dataOperation) {
				return Collections.singletonList(dataOperation);
			}
		});
        //pgsqlProtocol.getConfiguration().setMessagePartMaxLength(5);
        pgsqlProtocol.start();
        Thread.sleep(500);
        pgsqlProtocol.sync();
        return null;
    }

    public static void main(String[] args) throws Exception {
        new StartPgsqlServerProxy().call();
    }
}
