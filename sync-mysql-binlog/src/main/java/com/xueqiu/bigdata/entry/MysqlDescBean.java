package com.xueqiu.bigdata.entry;

public class MysqlDescBean {
    private String field;
    private String type;
    private String key;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return "MysqlDescBean{" +
                "field='" + field + '\'' +
                ", type='" + type + '\'' +
                ", key='" + key + '\'' +
                '}';
    }
}
