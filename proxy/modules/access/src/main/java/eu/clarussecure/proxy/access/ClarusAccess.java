package eu.clarussecure.proxy.access;

import org.apache.http.auth.UsernamePasswordCredentials;

public interface ClarusAccess {

	boolean authenticate(UsernamePasswordCredentials cr);

	boolean authenticate(String username, String password);

	boolean identify(String username);
}
