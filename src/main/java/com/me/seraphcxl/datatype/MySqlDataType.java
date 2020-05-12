package com.me.seraphcxl.datatype;

import org.apache.commons.lang3.StringUtils;

public enum MySqlDataType {
    TINYINT,
    SMALLINT,
    MEDIUMINT,
    INT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    YEAR,
    DATETIME,
    TIMESTAMP,
    CHAR,
    VARCHAR,
    TINYBLOB,
    TINYTEXT,
    BLOB,
    TEXT,
    MEDIUMBLOB,
    MEDIUMTEXT,
    LONGBLOB,
    LONGTEXT,
    BINARY,
    VARBINARY,
    BIT,
    REAL,
    NUMERIC,
    ENUM
    ;

    public static MySqlDataType toType(String typeStr) {
        MySqlDataType type = MySqlDataType.valueOf(typeStr.toUpperCase());
        return type;
    }

    public static MySqlDataType toTypeWithStr(String str) {
        MySqlDataType type = VARCHAR;
        if (StringUtils.isNotBlank(str)) {
            str = str.toLowerCase();
            if (str.startsWith("tinyint")) {
                type = TINYINT;
            } else if (str.startsWith("int")) {
                type = INT;
            } else if (str.startsWith("bigint")) {
                type = BIGINT;
            } else if (str.startsWith("varchar")) {
                type = VARCHAR;
            } else if (str.startsWith("decimal")) {
                type = DECIMAL;
            } else if (str.startsWith("float")) {
                type = FLOAT;
            } else if (str.startsWith("double")) {
                type = DOUBLE;
            } else if (str.startsWith("datetime")) {
                type = DATETIME;
            } else if (str.startsWith("timestamp")) {
                type = TIMESTAMP;
            } else if (str.startsWith("date")) {
                type = DATETIME;
            }

        }
        return type;
    }
}
