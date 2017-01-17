package eu.clarussecure.proxy.spi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MetadataOperation extends ModuleOperation {
    private Map<CString, List<CString>> metadata;

    public Map<CString, List<CString>> getMetadata() {
        if (metadata == null) {
            metadata = new LinkedHashMap<>();
        }
        return metadata;
    }

    public void setMetadata(Map<CString, List<CString>> metadata) {
        this.metadata = metadata;
    }

    @Override
    public List<CString> getDataIds() {
        return new ArrayList<>(getMetadata().keySet());
    }

    @Override
    public void setDataIds(List<CString> dataIds) {
        dataIds.forEach(dataId -> getMetadata().putIfAbsent(dataId, null));
    }

    @Override
    public void addDataId(CString dataId) {
        addDataId(dataId, null);
    }

    public void addDataId(CString dataId, List<CString> protectedDataIds) {
        getMetadata().put(dataId, protectedDataIds);
    }

    @Override
    public void removeDataId(CString dataId) {
        getMetadata().remove(dataId);
    }

    public List<CString> getProtectedDataIds(CString dataId) {
        return getMetadata().get(dataId);
    }

}
