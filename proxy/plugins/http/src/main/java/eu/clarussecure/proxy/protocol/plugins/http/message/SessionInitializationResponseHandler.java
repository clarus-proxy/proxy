package eu.clarussecure.proxy.protocol.plugins.http.message;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarussecure.proxy.protocol.plugins.http.message.ssl.SessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.http.message.ssl.SessionMessageTransferMode;
import io.netty.channel.ChannelHandlerContext;

public class SessionInitializationResponseHandler extends HttpMessageHandler<HttpSessionInitializationResponseMessage> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SessionInitializationResponseHandler.class);

	public SessionInitializationResponseHandler() {
		super(HttpSSLResponseMessage.class);
	}
	
	@Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
	 	getSessionInitializer(ctx).addSSLHandlerOnServerSide(ctx);
	 	getSessionInitializer(ctx).addSSLHandlerOnClientSide(ctx);
    }

	@Override
	protected HttpSessionInitializationResponseMessage process(ChannelHandlerContext ctx,
			HttpSessionInitializationResponseMessage msg) throws IOException {
		HttpSessionInitializationResponseMessage newMsg = msg;
		if (msg instanceof HttpSSLResponseMessage) {
			byte code = ((HttpSSLResponseMessage) msg).getCode();
			LOGGER.debug("SSL response: {}", code);
			SessionMessageTransferMode<Byte, Void> transferMode = getSessionInitializer(ctx).processSSLResponse(ctx,
					((HttpSSLResponseMessage) msg).getCode());

			switch (transferMode.getTransferMode()) {
			case FORWARD:
				// Forward the message
				if (transferMode.getNewDetails() != code) {
					LOGGER.trace("Modify the SSL response");
					newMsg = new HttpSSLResponseMessage(transferMode.getNewDetails());
				}
				LOGGER.trace("Forward the SSL response");
				break;
			case FORGET:
				// Don't forward the message
				LOGGER.trace("Ignore the SSL response");
				newMsg = null;
				break;
			case ERROR:
			case ORCHESTRATE:
			default:
				// Should not occur
				throw new IllegalArgumentException(
						"Invalid value for enum " + transferMode.getTransferMode().getClass().getSimpleName() + ": "
								+ transferMode.getTransferMode());
			}
		}
		return newMsg;
	}

	private SessionInitializer getSessionInitializer(ChannelHandlerContext ctx) {
		return getHttpSession(ctx).getSessionInitializer();
	}

}
