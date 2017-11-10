package eu.clarussecure.proxy.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InboundDataOperation extends DataOperation {
    private List<CString> clearDataIds;
    private List<Map<CString, CString>> dataIdMappings;

    public List<CString> getClearDataIds() {
        if (clearDataIds == null) {
            clearDataIds = new ArrayList<>();
        }
        return clearDataIds;
    }

    public void setClearDataIds(List<CString> clearDataIds) {
        this.clearDataIds = clearDataIds;
    }

    public void addClearDataId(CString clearDataId) {
        getClearDataIds().add(clearDataId);
    }

    public void addClearDataIds(List<CString> clearDataIds) {
        clearDataIds.forEach(clearDataId -> getClearDataIds().add(clearDataId));
    }

    public List<Map<CString, CString>> getDataIdMappings() {
        return dataIdMappings;
    }

    public void setDataIdMappings(List<Map<CString, CString>> dataIdMappings) {
        this.dataIdMappings = dataIdMappings;
    }
}
