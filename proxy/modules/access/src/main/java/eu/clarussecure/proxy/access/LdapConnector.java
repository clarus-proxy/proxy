package eu.clarussecure.proxy.access;

import java.io.IOException;
import java.util.Properties;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.ldap.sdk.SimpleBindRequest;

public final class LdapConnector implements ClarusAccess {

	private String host;
	private int port;
	private String bindDN;
	private String clarusUser;
	private String clarusPass;

	public LdapConnector() throws IOException {
		Properties p = new Properties();
		p.load(getClass().getClassLoader().getResourceAsStream("ldap.properties"));

		this.host = p.getProperty("host");
		this.port = Integer.parseInt(p.getProperty("port"));
		this.bindDN = p.getProperty("bindDN");
		this.clarusUser = p.getProperty("clarusUser");
		this.clarusPass = p.getProperty("clarusPass");
	}

	@Override
	public boolean authenticate(String username, String password) {
		try {
			LDAPConnection conn = new LDAPConnection(this.host, this.port);

			SimpleBindRequest r = new SimpleBindRequest("cn=" + username + "," + this.bindDN, password);
			conn.bind(r);

			conn.close();
			return true;
		} catch (LDAPException e) {
			return false;
		}
	}

	@Override
	public boolean identify(String username) {
		try {
			LDAPConnection conn = new LDAPConnection(this.host, this.port, "cn=" + this.clarusUser + "," + bindDN, this.clarusPass);

			SearchRequest re = new SearchRequest("dc=local,dc=net", SearchScope.SUB, "(cn=*)");
			SearchResult searchResult = conn.search(re);

			for (SearchResultEntry e : searchResult.getSearchEntries()) {
				Attribute cn = e.getAttribute("cn");

				if (cn.getValue().equalsIgnoreCase(username)) {
					conn.close();
					return true;
				}
			}

			conn.close();

			return false;
		} catch (LDAPException e) {
			return false;
		}
	}

	public static void main(String[] args) throws Exception {
		LdapConnector c = new LdapConnector();
		System.out.println(c.authenticate("Stefan Janacek", "pass"));
		System.out.println(c.identify("Stefan Janacek"));
	}
}
