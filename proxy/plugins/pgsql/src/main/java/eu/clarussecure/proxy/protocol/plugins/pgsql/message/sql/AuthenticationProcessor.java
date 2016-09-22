package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Map;

import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlConstants;
import eu.clarussecure.proxy.protocol.plugins.pgsql.PgsqlSession;
import eu.clarussecure.proxy.spi.CString;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticationProcessor {

    public static final String USER_KEY = "user";
    public static final String DATABASE_KEY = "database";

    public void processAuthentication(ChannelHandlerContext ctx, Map<CString, CString> parameters) {
        CString databaseName = parameters.get(CString.valueOf(DATABASE_KEY));
        getSession(ctx).setDatabaseName(databaseName);
    }

    private SqlSession getSession(ChannelHandlerContext ctx) {
        PgsqlSession pgsqlSession = ctx.channel().attr(PgsqlConstants.SESSION_KEY).get();
        return pgsqlSession.getSession();
    }

}
