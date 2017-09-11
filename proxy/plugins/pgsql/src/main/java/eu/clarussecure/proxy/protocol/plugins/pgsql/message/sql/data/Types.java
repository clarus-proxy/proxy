package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Types {

    // Map Oid to type
    private final Map<Long, Type> oid2Types;
    private final List<Type> typesWithUnfixedOid;

    {
        oid2Types = new HashMap<>(Type.values().length);
        typesWithUnfixedOid = new ArrayList<>(Type.values().length);
        for (Type type : Type.values()) {
            if (type.getOid() != -1) {
                oid2Types.put(type.getOid(), type);
            } else {
                typesWithUnfixedOid.add(type);
            }
        }
    }

    // Map name to type
    private final Map<String, Type> name2Types;

    {
        name2Types = new HashMap<>(Type.values().length);
        for (Type type : Type.values()) {
            name2Types.put(type.getName(), type);
        }
    }

    public synchronized void setTypeOid(String name, long oid) {
        Type type = getType(name);
        if (type != null && type.getOid() != oid) {
            if (type.getOid() != -1) {
                oid2Types.remove(type.getOid());
            }
            type.setOid(oid);
            oid2Types.put(type.getOid(), type);
            typesWithUnfixedOid.remove(type);
        }
    }

    public Type getType(long typeID) {
        Type type = oid2Types.get(typeID);
        return type;
    }

    public Type getType(String name) {
        Type type = name2Types.get(name);
        return type;
    }

    public Map<Long, Type> getOid2Types() {
        return oid2Types;
    }

    public List<Type> getTypesWithUnfixedOid() {
        return typesWithUnfixedOid;
    }
}
