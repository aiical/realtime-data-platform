package com.xueqiu.bigdata.entry;

import org.apache.kudu.Type;

public class ZkDataBean {
    private String mysqlType;
    private Type kuduType;
    private boolean isPk;

    public ZkDataBean(){

    }

    public ZkDataBean(String mysqlType, Type kuduType, boolean isPk){
        this.mysqlType = mysqlType;
        this.kuduType = kuduType;
        this.isPk = isPk;
    }

    public String getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(String mysqlType) {
        this.mysqlType = mysqlType;
    }

    public Type getKuduType() {
        return kuduType;
    }

    public void setKuduType(Type kuduType) {
        this.kuduType = kuduType;
    }

    public boolean isPk() {
        return isPk;
    }

    public void setPk(boolean pk) {
        isPk = pk;
    }

    @Override
    public String toString() {
        return "ZkDataBean{" +
                "mysqlType='" + mysqlType + '\'' +
                ", kuduType=" + kuduType +
                ", isPk=" + isPk +
                '}';
    }
}
