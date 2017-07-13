package eu.clarussecure.proxy.spi;

import java.util.ArrayList;
import java.util.List;

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

    private List<Criterion> criterion;

    public List<Criterion> getCriterions() {
        if (criterion == null) {
            criterion = new ArrayList<>();
        }
        return criterion;
    }

    public void setCriterions(List<Criterion> criterion) {
        this.criterion = criterion;
    }

    public void addCriterion(Criterion criterion) {
        getCriterions().add(criterion);
    }

}
