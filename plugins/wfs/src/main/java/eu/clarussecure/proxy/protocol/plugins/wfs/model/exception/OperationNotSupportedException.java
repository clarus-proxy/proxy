package eu.clarussecure.proxy.protocol.plugins.wfs.model.exception;

public class OperationNotSupportedException extends Exception {

    private static final long serialVersionUID = 7436861613055693147L;

    public OperationNotSupportedException() {
        super();
    }

    public OperationNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public OperationNotSupportedException(String message) {
        super(message);
    }

    public OperationNotSupportedException(Throwable cause) {
        super(cause);
    }

}