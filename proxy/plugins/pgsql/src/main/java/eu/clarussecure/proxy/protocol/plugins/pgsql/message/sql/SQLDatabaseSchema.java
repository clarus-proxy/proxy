package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data.Types;

public class SQLDatabaseSchema {

    private Map<String, List<String>> datasetDefinitions;
    private Map<String, String> datasetSrids;
    private Map<Long, SortedSet<Integer>> typeOIDBackends;
    private SortedMap<Integer, Types> backendTypes;
    private Types types;

    public Map<String, List<String>> getDatasetDefinitions() {
        if (datasetDefinitions == null) {
            datasetDefinitions = Collections.synchronizedMap(new HashMap<>());
        }
        return datasetDefinitions;
    }

    public void setDatasetDefinitions(Map<String, List<String>> datasetDefinitions) {
        this.datasetDefinitions = datasetDefinitions;
    }

    public void addDatasetDefinition(String datasetId, List<String> dataIds) {
        if (datasetId.endsWith("/")) {
            datasetId = datasetId.substring(0, datasetId.length() - 1);
        }
        getDatasetDefinitions().put(datasetId, dataIds);
    }

    public void removeDatasetDefinition(String datasetId) {
        if (datasetId.endsWith("/")) {
            datasetId = datasetId.substring(0, datasetId.length() - 1);
        }
        getDatasetDefinitions().remove(datasetId);
    }

    public void resetDatasetDefinition() {
        if (datasetDefinitions != null) {
            datasetDefinitions.clear();
        }
    }

    public List<String> getDatasetDefinition(String datasetId) {
        if (datasetId.endsWith("/")) {
            datasetId = datasetId.substring(0, datasetId.length() - 1);
        }
        return getDatasetDefinitions().get(datasetId);
    }

    public Map<String, String> getDatasetSrids() {
        if (datasetSrids == null) {
            datasetSrids = Collections.synchronizedMap(new HashMap<>());
        }
        return datasetSrids;
    }

    public void setDatasetSrids(Map<String, String> datasetSrids) {
        this.datasetSrids = datasetSrids;
    }

    public void addDatasetSrid(String datasetId, String srid) {
        if (datasetId.endsWith("/")) {
            datasetId = datasetId.substring(0, datasetId.length() - 1);
        }
        getDatasetSrids().put(datasetId, srid);
    }

    public void removeDatasetSrid(String datasetId) {
        if (datasetId.endsWith("/")) {
            datasetId = datasetId.substring(0, datasetId.length() - 1);
        }
        getDatasetSrids().remove(datasetId);
    }

    public void resetDatasetSrids() {
        if (datasetSrids != null) {
            datasetSrids.clear();
        }
    }

    public String getDatasetSrid(String datasetId) {
        if (datasetId.endsWith("/")) {
            datasetId = datasetId.substring(0, datasetId.length() - 1);
        }
        return getDatasetSrids().get(datasetId);
    }

    public Map<Long, SortedSet<Integer>> getTypeOIDBackends() {
        if (typeOIDBackends == null) {
            typeOIDBackends = new ConcurrentHashMap<>();
        }
        return typeOIDBackends;
    }

    public void setTypeOIDBackends(Map<Long, SortedSet<Integer>> typeOIDBackends) {
        this.typeOIDBackends = typeOIDBackends;
    }

    public void addTypeOIDBackend(long typeOID, int backend) {
        SortedSet<Integer> backends = getTypeOIDBackends().get(typeOID);
        if (backends == null) {
            synchronized (getTypeOIDBackends()) {
                backends = getTypeOIDBackends().get(typeOID);
                if (backends == null) {
                    backends = new TreeSet<>();
                    getTypeOIDBackends().put(typeOID, backends);
                }
            }
        }
        backends.add(backend);
    }

    public SortedSet<Integer> getTypeOIDBackends(long typeOID) {
        return getTypeOIDBackends().get(typeOID);
    }

    public synchronized SortedMap<Integer, Types> getAllBackendTypes() {
        if (backendTypes == null) {
            backendTypes = new TreeMap<>();
        }
        return backendTypes;
    }

    public synchronized Types getBackendTypes(int backend) {
        Types types = getAllBackendTypes().computeIfAbsent(backend, k -> new Types());
        return types;
    }

    public synchronized void setBackendTypes(SortedMap<Integer, Types> backendTypes) {
        this.backendTypes = backendTypes;
    }

    public void resetBackendTypes() {
        backendTypes = null;
    }

    public synchronized Types getTypes() {
        if (types == null) {
            types = new Types();
        }
        return types;
    }

    public synchronized void setTypes(Types types) {
        this.types = types;
    }
}
