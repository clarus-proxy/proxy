package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data;

public class PGArray {
    private int ndims;
    private boolean hasnull;
    private long typeOid;
    private int[] dims;
    private int[] lbounds;
    private char[] separators;
    private Object array;

    public PGArray() {
    }

    public PGArray(int ndims, boolean hasnull, long typeOid, int[] dims, int[] lbounds, char[] separators,
            Object array) {
        this.ndims = ndims;
        this.hasnull = hasnull;
        this.typeOid = typeOid;
        this.dims = dims;
        this.lbounds = lbounds;
        this.separators = separators;
        this.array = array;
    }

    public int getNdims() {
        return ndims;
    }

    public void setNdims(int ndims) {
        this.ndims = ndims;
    }

    public boolean isHasnull() {
        return hasnull;
    }

    public void setHasnull(boolean hasnull) {
        this.hasnull = hasnull;
    }

    public long getTypeOid() {
        return typeOid;
    }

    public void setTypeOid(long typeOid) {
        this.typeOid = typeOid;
    }

    public int[] getDims() {
        return dims;
    }

    public void setDims(int[] dims) {
        this.dims = dims;
    }

    public int[] getLbounds() {
        return lbounds;
    }

    public void setLbounds(int[] lbounds) {
        this.lbounds = lbounds;
    }

    public char[] getSeparators() {
        return separators;
    }

    public void setSeparators(char[] separators) {
        this.separators = separators;
    }

    public Object getArray() {
        return array;
    }

    public void setArray(Object array) {
        this.array = array;
    }
}