package eu.clarussecure.proxy.spi;

import java.util.List;

public abstract class ModuleOperation {
    protected boolean modified;

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    public abstract List<CString> getDataIds();

    public abstract void setDataIds(List<CString> dataIds);

    public abstract void addDataId(CString dataId);

    public abstract void removeDataId(CString dataId);
}
