package eu.clarussecure.proxy.spi;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ModuleOperation {
    protected boolean modified;
    protected Map<String, Object> attributes;
    protected List<Integer> involvedCSPs;

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    public Map<String, Object> getAttributes() {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
        return attributes;
    }

    public Object getAttribute(String key) {
        return getAttributes().get(key);
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public void addAttribute(String key, Object value) {
        getAttributes().put(key, value);
    }

    public Object removeAttribute(String key) {
        return getAttributes().remove(key);
    }

    public abstract List<CString> getDataIds();

    public abstract void setDataIds(List<CString> dataIds);

    public abstract void addDataId(CString dataId);

    public abstract void addDataIds(List<CString> dataIds);

    public abstract void removeDataId(int index);

    public List<Integer> getInvolvedCSPs() {
        return involvedCSPs;
    }

    public void setInvolvedCSPs(List<Integer> involvedCSPs) {
        this.involvedCSPs = involvedCSPs;
    }

    public int getInvolvedCSP() {
        return involvedCSPs != null && involvedCSPs.size() > 0 ? involvedCSPs.get(0) : -1;
    }

    public void setInvolvedCSP(int involvedCSP) {
        this.involvedCSPs = Collections.singletonList(involvedCSP);
    }
}
