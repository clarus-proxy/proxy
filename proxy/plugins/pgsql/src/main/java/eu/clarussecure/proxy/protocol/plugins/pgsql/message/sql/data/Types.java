package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data;

import java.util.HashMap;
import java.util.Map;

public class Types {

    // Map Oid to type
    private static final Map<Long, Type> OID_2_TYPES;

    static {
        OID_2_TYPES = new HashMap<>(Type.values().length);
        for (Type type : Type.values()) {
            if (type.getOid() != -1) {
                OID_2_TYPES.put(type.getOid(), type);
            }
        }
    }

    // Map name to type
    private static final Map<String, Type> NAME_2_TYPES;

    static {
        NAME_2_TYPES = new HashMap<>(Type.values().length);
        for (Type type : Type.values()) {
            NAME_2_TYPES.put(type.getName(), type);
        }
    }

    public static void setTypeOid(String name, long oid) {
        Type type = getType(name);
        if (type != null && type.getOid() != oid) {
            if (type.getOid() != -1) {
                OID_2_TYPES.remove(type.getOid());
            }
            type.setOid(oid);
            OID_2_TYPES.put(type.getOid(), type);
        }
    }

    public static Type getType(long typeID) {
        Type type = OID_2_TYPES.get(typeID);
        return type;
    }

    public static Type getType(String name) {
        Type type = NAME_2_TYPES.get(name);
        return type;
    }

}
