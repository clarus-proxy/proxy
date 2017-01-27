package eu.clarussecure.proxy.spi;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetadataOperation extends ModuleOperation {

    private List<Map.Entry<CString, List<CString>>> metadata;

    public List<Map.Entry<CString, List<CString>>> getMetadata() {
        if (metadata == null) {
            metadata = new ArrayList<>();
        }
        return metadata;
    }

    public void setMetadata(List<Map.Entry<CString, List<CString>>> metadata) {
        this.metadata = metadata;
    }

    @Override
    public List<CString> getDataIds() {
        return getMetadata().stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    @Override
    public void setDataIds(List<CString> dataIds) {
        getMetadata().clear();
        dataIds.forEach(dataId -> {
            getMetadata().add(new SimpleEntry<>(dataId, null));
        });
    }

    @Override
    public void addDataId(CString dataId) {
        addDataId(dataId, null);
    }

    public void addDataId(CString dataId, List<CString> protectedDataIds) {
        getMetadata().add(new SimpleEntry<>(dataId, protectedDataIds));
    }

    @Override
    public void addDataIds(List<CString> dataIds) {
        dataIds.forEach(dataId -> addDataId(dataId, null));
    }

    @Override
    public void removeDataId(int index) {
        getMetadata().remove(index);
    }

    public List<CString> getProtectedDataIds(int index) {
        return getMetadata().get(index).getValue();
    }

}
