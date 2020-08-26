package com.xueqiu.bigdata.entry;

import org.apache.kudu.Type;

public class KuduColumnBean {
    private String cName;
    private Type cType;
    private boolean isKey;

    public KuduColumnBean(){

    }

    public KuduColumnBean(String cName, Type cType, Boolean isKey){
        this.cName = cName;
        this.cType = cType;
        this.isKey = isKey;
    }

    public String getcName() {
        return cName;
    }

    public void setcName(String cName) {
        this.cName = cName;
    }

    public Type getcType() {
        return cType;
    }

    public void setcType(Type cType) {
        this.cType = cType;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    @Override
    public String toString() {
        return "KuduColumnBean{" +
                "cName='" + cName + '\'' +
                ", cType=" + cType +
                ", isKey=" + isKey +
                '}';
    }
}
