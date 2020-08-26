package com.xueqiu.bigdata.util;

import org.apache.kudu.Type;

public class KuduUtils {

    public static Type getKuduType(String rawType){
        Type resultType;
        String matchStr = rawType.split("（")[0].toLowerCase();

        switch (matchStr){
            case "tinyint":
                resultType = Type.INT8;
                break;
            case "smallint":
                resultType = Type.INT16;
                break;
            case "mediumint":
            case "int":
            case "integer":
                resultType = Type.INT32;
                break;
            case "bigint":
                resultType = Type.INT64;
                break;
            case "float":
                resultType = Type.FLOAT;
                break;
            case "double":
                resultType = Type.DOUBLE;
                break;
            case "decimal":
                resultType = Type.DECIMAL;
                break;
            case "char":
            case "varchar":
                resultType = Type.STRING;
                break;
            case "binary":
                resultType = Type.BINARY;
                break;
            case "timestamp":
                resultType = Type.UNIXTIME_MICROS;
                break;
            default:
                resultType = Type.STRING;
                break;
        }

        return resultType;
    }
}
