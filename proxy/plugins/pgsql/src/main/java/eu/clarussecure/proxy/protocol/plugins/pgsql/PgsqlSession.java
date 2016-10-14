package eu.clarussecure.proxy.protocol.plugins.pgsql;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.EventProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.NoopEventProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.PgsqlEventProcessor;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SqlSession;
import eu.clarussecure.proxy.protocol.plugins.tcp.TCPSession;

public class PgsqlSession extends TCPSession {

    private SqlSession session;
    
    private EventProcessor eventProcessor;

    public SqlSession getSession() {
        if (session == null) {
            session = new SqlSession();
        }
        return session;
    }

    public EventProcessor getEventProcessor() {
        if (eventProcessor == null) {
            String eventProcessing = System.getProperty("pgsql.event.processing");
            if (eventProcessing != null && (Boolean.FALSE.toString().equalsIgnoreCase(eventProcessing) || "0".equalsIgnoreCase(eventProcessing) || "no".equalsIgnoreCase(eventProcessing) || "off".equalsIgnoreCase(eventProcessing))) {
                eventProcessor = new NoopEventProcessor();
            } else {
                eventProcessor = new PgsqlEventProcessor();
            }
        }
        return eventProcessor;
    }

    public void setEventProcessor(EventProcessor commandProcessor) {
        this.eventProcessor = commandProcessor;
    }
}
