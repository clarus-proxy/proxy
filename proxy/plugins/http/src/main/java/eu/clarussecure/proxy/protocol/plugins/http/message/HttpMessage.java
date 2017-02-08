package eu.clarussecure.proxy.protocol.plugins.http.message;

public interface HttpMessage {

	byte getType();

	int getHeaderSize();
}
