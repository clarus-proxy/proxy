package eu.clarussecure.proxy.protocol.plugins.wfs.model.exception;

public class ProtocolVersionNotSupportedException extends Exception {

    private static final long serialVersionUID = -7160597018810454914L;

    public ProtocolVersionNotSupportedException() {
        super();
    }

    public ProtocolVersionNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtocolVersionNotSupportedException(String message) {
        super(message);
    }

    public ProtocolVersionNotSupportedException(Throwable cause) {
        super(cause);
    }

}