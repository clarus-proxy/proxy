package eu.clarussecure.proxy.protocol.plugins.http;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import eu.clarussecure.proxy.protocol.plugins.http.handlers.HttpRequestDecoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

public class FrontendSidePipelineInitializer extends ChannelInitializer<Channel> {

	private SslContext sslCtx;
	private static final boolean SSL = System.getProperty("ssl") != null;

	public FrontendSidePipelineInitializer(SslContext sslCtx) {
		this.sslCtx = sslCtx;
	}

	public FrontendSidePipelineInitializer() {
		try {
			if (SSL) {
				SelfSignedCertificate ssc = new SelfSignedCertificate();
				sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
			} else {
				sslCtx = null;
			}
		} catch (CertificateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sslCtx = null;
		} catch (SSLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sslCtx = null;
		}
	}

	@Override
	public void initChannel(Channel ch) {
		ChannelPipeline p = ch.pipeline();
		
		// Enable HTTPS if necessary.
		if (sslCtx != null) {
			p.addLast(sslCtx.newHandler(ch.alloc()));
		}

		//p.addLast(new HttpClientCodec());
		// Remove the following line if you don't want automatic content
		// decompression.
		//p.addLast(new HttpContentDecompressor());
		// Uncomment the following line if you don't want to handle
		// HttpContents.
		// p.addLast(new HttpObjectAggregator(1048576));
		p.addLast(new HttpRequestDecoder());

	}
}
