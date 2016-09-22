package eu.clarussecure.proxy.protocol.plugins.pgsql.parser;

import org.junit.Test;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class ParserTest {

    @Test
    public void testQueries() throws Exception {
        Statement statement = null;
        statement = CCJSqlParserUtil.parse("SET CLIENT_ENCODING TO UTF8;");
        statement = CCJSqlParserUtil.parse("SET STANDARD_CONFORMING_STRINGS TO ON;");
        statement = CCJSqlParserUtil.parse("BEGIN;");
        statement = CCJSqlParserUtil.parse("CREATE TABLE \"public\".\"toulouse_osm_places\" (gid serial,"
                                         + "\"osm_id\" varchar(10),"
                                         + "\"lastchange\" varchar(20),"
                                         + "\"code\" int2,"
                                         + "\"fclass\" varchar(20),"
                                         + "\"geomtype\" varchar(1),"
                                         + "\"name\" varchar(100),"
                                         + "\"population\" int4);");
        statement = CCJSqlParserUtil.parse("ALTER TABLE \"public\".\"toulouse_osm_places\" ADD PRIMARY KEY (gid);");
        statement = CCJSqlParserUtil.parse("SELECT AddGeometryColumn('public','toulouse_osm_places','geom','4326','POINT',2);");
        statement = CCJSqlParserUtil.parse("INSERT INTO \"public\".\"toulouse_osm_places\" (\"osm_id\",\"lastchange\",\"code\",\"fclass\",\"geomtype\",\"name\",\"population\",geom) VALUES ('26686518','2015-08-13T04:23:30Z','1001','city','N','Toulouse','441802','0101000020E610000012972DA3A21BF73F885572045FCD4540');");
        statement = CCJSqlParserUtil.parse("CREATE INDEX \"toulouse_osm_places_geom_gist\" ON \"public\".\"toulouse_osm_places\" USING GIST (\"geom\");");
        statement = CCJSqlParserUtil.parse("COMMIT;");
    }

}
