package eu.clarussecure.proxy.access;

public interface ClarusAccess {

	boolean authenticate(String username, String password);

	boolean identify(String username);
}
