package eu.clarussecure.proxy.protocol.plugins.http.message;

import java.util.Map;
import java.util.Objects;

import eu.clarussecure.proxy.spi.CString;
import io.netty.util.internal.StringUtil;

public class HttpStartupMessage implements HttpSessionInitializationRequestMessage {

	public static final byte TYPE = (byte) 0;
	public static final int HEADER_SIZE = Integer.BYTES;

	private int protocolVersion;
	private Map<CString, CString> parameters;

	public HttpStartupMessage(int protocolVersion, Map<CString, CString> parameters) {
		this.protocolVersion = protocolVersion;
		this.parameters = Objects.requireNonNull(parameters, "parameters must not be null");
	}

	public int getProtocolVersion() {
		return protocolVersion;
	}

	public String getProtocolVersionAsString() {
		return String.format("%d.%d", getProtocolMajorVersion(), getProtocolMinorVersion());
	}

	public int getProtocolMajorVersion() {
		return (protocolVersion >> 16) & 0xffff;
	}

	public int getProtocolMinorVersion() {
		return protocolVersion & 0xffff;
	}

	public void setProtocolVersion(int protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public Map<CString, CString> getParameters() {
		return parameters;
	}

	public void setParameters(Map<CString, CString> parameters) {
		this.parameters = parameters;
	}

	@Override
	public byte getType() {
		return TYPE;
	}

	@Override
	public int getHeaderSize() {
		return HEADER_SIZE;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(StringUtil.simpleClassName(this));
		builder.append(" [");
		builder.append("protocolVersion=").append(getProtocolVersionAsString());
		builder.append(", parameters=").append(parameters);
		builder.append("]");
		return builder.toString();
	}

}
