package eu.clarussecure.proxy.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OutboundDataOperation extends DataOperation {
    public static class Criterion {
        private CString dataId;
        private CString operator;
        private CString value;

        public Criterion(CString dataId, CString operator, CString value) {
            this.dataId = dataId;
            this.operator = operator;
            this.value = value;
        }

        public CString getDataId() {
            return dataId;
        }

        public CString getOperator() {
            return operator;
        }

        public CString getValue() {
            return value;
        }

        public void setDataId(CString dataId) {
            this.dataId = dataId;
        }

        public void setOperator(CString operator) {
            this.operator = operator;
        }

        public void setValue(CString value) {
            this.value = value;
        }
    }

    private Map<CString, CString> dataIdMapping;
    private List<Criterion> criterions;

    public Map<CString, CString> getDataIdMapping() {
        return dataIdMapping;
    }

    public void setDataIdMapping(Map<CString, CString> dataIdMapping) {
        this.dataIdMapping = dataIdMapping;
    }

    public List<Criterion> getCriterions() {
        if (criterions == null) {
            criterions = new ArrayList<>();
        }
        return criterions;
    }

    public void setCriterions(List<Criterion> criterions) {
        this.criterions = criterions;
    }

    public void addCriterions(List<Criterion> criterions) {
        criterions.forEach(criterion -> addCriterion(criterion));
    }

    public void addCriterion(Criterion criterion) {
        getCriterions().add(criterion);
    }

}
