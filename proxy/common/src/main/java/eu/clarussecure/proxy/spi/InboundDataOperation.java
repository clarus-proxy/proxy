package eu.clarussecure.proxy.spi;

import java.util.ArrayList;
import java.util.List;

public class InboundDataOperation extends DataOperation {
    private List<CString> clearDataIds;

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

}
