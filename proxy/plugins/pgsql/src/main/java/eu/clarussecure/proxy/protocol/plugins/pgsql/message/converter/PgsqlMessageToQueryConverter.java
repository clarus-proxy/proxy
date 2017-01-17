package eu.clarussecure.proxy.protocol.plugins.pgsql.message.converter;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlBindMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlCloseMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlDescribeMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlExecuteMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlFlushMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlParseMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlQueryRequestMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSimpleQueryMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlSyncMessage;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.BindStep;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.CloseStep;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.DescribeStep;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.ExecuteStep;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.ExtendedQuery;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.FlushStep;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.ParseStep;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.Query;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SQLStatement;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SimpleQuery;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SimpleSQLStatement;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.SynchronizeStep;
import io.netty.util.internal.StringUtil;

public class PgsqlMessageToQueryConverter {

    public static Query from(PgsqlQueryRequestMessage msg) {
        if (msg instanceof PgsqlQueryMessage) {
            return from((PgsqlQueryMessage) msg);
        } else if (msg instanceof PgsqlBindMessage) {
            return from((PgsqlBindMessage) msg);
        } else if (msg instanceof PgsqlDescribeMessage) {
            return from((PgsqlDescribeMessage) msg);
        } else if (msg instanceof PgsqlExecuteMessage) {
            return from((PgsqlExecuteMessage) msg);
        } else if (msg instanceof PgsqlCloseMessage) {
            return from((PgsqlCloseMessage) msg);
        } else if (msg instanceof PgsqlSyncMessage) {
            return from((PgsqlSyncMessage) msg);
        } else if (msg instanceof PgsqlFlushMessage) {
            return from((PgsqlFlushMessage) msg);
        }
        throw new IllegalArgumentException(StringUtil.simpleClassName(msg) + " not supported");
    }

    public static PgsqlQueryRequestMessage to(Query query) {
        if (query instanceof SimpleQuery) {
            return to((SimpleQuery) query);
        } else {
            return to((ExtendedQuery) query);
        }
    }

    public static PgsqlQueryRequestMessage to(SimpleQuery query) {
        return to((SimpleSQLStatement) query);
    }

    public static PgsqlQueryRequestMessage to(ExtendedQuery query) {
        if (query instanceof ParseStep) {
            return to((ParseStep) query);
        } else if (query instanceof BindStep) {
            return to((BindStep) query);
        } else if (query instanceof DescribeStep) {
            return to((DescribeStep) query);
        } else if (query instanceof ExecuteStep) {
            return to((ExecuteStep) query);
        } else if (query instanceof CloseStep) {
            return to((CloseStep) query);
        } else if (query instanceof SynchronizeStep) {
            return to((SynchronizeStep) query);
        } else if (query instanceof FlushStep) {
            return to((FlushStep) query);
        }
        throw new IllegalArgumentException(StringUtil.simpleClassName(query) + " not supported");
    }

    public static SQLStatement from(PgsqlQueryMessage msg) {
        if (msg instanceof PgsqlSimpleQueryMessage) {
            return from((PgsqlSimpleQueryMessage) msg);
        } else {
            return from((PgsqlParseMessage) msg);
        }
    }

    public static PgsqlQueryMessage to(SQLStatement sqlStatement) {
        if (sqlStatement instanceof SimpleSQLStatement) {
            return to((SimpleSQLStatement) sqlStatement);
        } else {
            return to((ParseStep) sqlStatement);
        }
    }

    public static SimpleSQLStatement from(PgsqlSimpleQueryMessage msg) {
        return new SimpleSQLStatement(msg.getQuery());
    }

    public static PgsqlSimpleQueryMessage to(SimpleSQLStatement sqlStatement) {
        return new PgsqlSimpleQueryMessage(sqlStatement.getSQL());
    }

    public static ParseStep from(PgsqlParseMessage msg) {
        return new ParseStep(msg.getPreparedStatement(), msg.getQuery(), msg.getParameterTypes());
    }

    public static PgsqlParseMessage to(ParseStep query) {
        return new PgsqlParseMessage(query.getName(), query.getSQL(), query.getParameterTypes());
    }

    public static BindStep from(PgsqlBindMessage msg) {
        return new BindStep(msg.getPortal(), msg.getPreparedStatement(), msg.getParameterFormats(),
                msg.getParameterValues(), msg.getResultColumnFormats());
    }

    public static PgsqlBindMessage to(BindStep query) {
        return new PgsqlBindMessage(query.getName(), query.getPreparedStatement(), query.getParameterFormats(),
                query.getParameterValues(), query.getResultColumnFormats());
    }

    public static DescribeStep from(PgsqlDescribeMessage msg) {
        return new DescribeStep(msg.getCode(), msg.getName());
    }

    public static PgsqlDescribeMessage to(DescribeStep query) {
        return new PgsqlDescribeMessage(query.getCode(), query.getName());
    }

    public static ExecuteStep from(PgsqlExecuteMessage msg) {
        return new ExecuteStep(msg.getPortal(), msg.getMaxRows());
    }

    public static PgsqlExecuteMessage to(ExecuteStep query) {
        return new PgsqlExecuteMessage(query.getPortal(), query.getMaxRows());
    }

    public static CloseStep from(PgsqlCloseMessage msg) {
        return new CloseStep(msg.getCode(), msg.getName());
    }

    public static PgsqlCloseMessage to(CloseStep query) {
        return new PgsqlCloseMessage(query.getCode(), query.getName());
    }

    public static SynchronizeStep from(PgsqlSyncMessage msg) {
        return new SynchronizeStep();
    }

    public static PgsqlSyncMessage to(SynchronizeStep query) {
        return new PgsqlSyncMessage();
    }

    public static FlushStep from(PgsqlFlushMessage msg) {
        return new FlushStep();
    }

    public static PgsqlFlushMessage to(FlushStep query) {
        return new PgsqlFlushMessage();
    }

}
