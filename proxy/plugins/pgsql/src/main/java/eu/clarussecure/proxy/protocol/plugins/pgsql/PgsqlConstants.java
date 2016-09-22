package eu.clarussecure.proxy.protocol.plugins.pgsql;

import io.netty.util.AttributeKey;

public interface PgsqlConstants {

    AttributeKey<PgsqlConfiguration> CONFIGURATION_KEY = AttributeKey.<PgsqlConfiguration>newInstance("CONFIGURATION_KEY");

    AttributeKey<PgsqlSession> SESSION_KEY = AttributeKey.<PgsqlSession>newInstance("SESSION_KEY");
}
