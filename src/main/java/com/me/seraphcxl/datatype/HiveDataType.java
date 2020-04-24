package com.me.seraphcxl.datatype;

/**
 * @author xiaoliangchen
 */

public enum HiveDataType {
    STRING
    , INT
    , BIGINT
    , DOUBLE
    , DECIMAL
    , DATETIME
    , TIMESTAMP
    ;

    public static HiveDataType toType(String typeStr) {
        HiveDataType type = HiveDataType.valueOf(typeStr.toUpperCase());
        return type;
    }
}
