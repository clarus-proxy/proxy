package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql.data;

public class GBox {
    private short flags;
    private double xmin;
    private double xmax;
    private double ymin;
    private double ymax;
    private double zmin;
    private double zmax;
    private double mmin;
    private double mmax;

    public GBox() {
    }

    public GBox(short flags, double xmin, double xmax, double ymin, double ymax, double zmin, double zmax, double mmin,
            double mmax) {
        super();
        this.flags = flags;
        this.xmin = xmin;
        this.xmax = xmax;
        this.ymin = ymin;
        this.ymax = ymax;
        this.zmin = zmin;
        this.zmax = zmax;
        this.mmin = mmin;
        this.mmax = mmax;
    }

    public short getFlags() {
        return flags;
    }

    public void setFlags(short flags) {
        this.flags = flags;
    }

    public double getXmin() {
        return xmin;
    }

    public void setXmin(double xmin) {
        this.xmin = xmin;
    }

    public double getXmax() {
        return xmax;
    }

    public void setXmax(double xmax) {
        this.xmax = xmax;
    }

    public double getYmin() {
        return ymin;
    }

    public void setYmin(double ymin) {
        this.ymin = ymin;
    }

    public double getYmax() {
        return ymax;
    }

    public void setYmax(double ymax) {
        this.ymax = ymax;
    }

    public double getZmin() {
        return zmin;
    }

    public void setZmin(double zmin) {
        this.zmin = zmin;
    }

    public double getZmax() {
        return zmax;
    }

    public void setZmax(double zmax) {
        this.zmax = zmax;
    }

    public double getMmin() {
        return mmin;
    }

    public void setMmin(double mmin) {
        this.mmin = mmin;
    }

    public double getMmax() {
        return mmax;
    }

    public void setMmax(double mmax) {
        this.mmax = mmax;
    }

}