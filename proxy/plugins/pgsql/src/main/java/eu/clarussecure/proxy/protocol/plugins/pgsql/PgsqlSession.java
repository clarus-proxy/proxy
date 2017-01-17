package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.EventProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.NoopEventProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.PgsqlEventProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLSession;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.ssl.SessionInitializer;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;

public class PgsqlSession extends TCPSession {

    private static boolean EVENT_PROCESSING_ACTIVATED;
    static {
        String eventProcessing = System.getProperty("pgsql.event.processing", "true");
        EVENT_PROCESSING_ACTIVATED = Boolean.TRUE.toString().equalsIgnoreCase(eventProcessing)
                || "1".equalsIgnoreCase(eventProcessing) || "yes".equalsIgnoreCase(eventProcessing)
                || "on".equalsIgnoreCase(eventProcessing);
    }

    private SessionInitializer sessionInitializer;

    private SQLSession sqlSession;

    private EventProcessor eventProcessor;

    public SessionInitializer getSessionInitializer() {
        if (sessionInitializer == null) {
            sessionInitializer = new SessionInitializer();
        }
        return sessionInitializer;
    }

    public SQLSession getSqlSession() {
        if (sqlSession == null) {
            sqlSession = new SQLSession();
        }
        return sqlSession;
    }

    public EventProcessor getEventProcessor() {
        if (eventProcessor == null) {
            eventProcessor = EVENT_PROCESSING_ACTIVATED ? new PgsqlEventProcessor() : new NoopEventProcessor();
        }
        return eventProcessor;
    }
}
