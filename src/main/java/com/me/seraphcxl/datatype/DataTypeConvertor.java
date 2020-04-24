package com.me.seraphcxl.datatype;

/**
 * @author xiaoliangchen
 */
public class DataTypeConvertor {
    public static HiveDataType convMySql2Hive(MySqlDataType mySqlDataType) {
        HiveDataType result = HiveDataType.STRING;
        do {
            switch (mySqlDataType) {
                case BIGINT: {
                    result =  HiveDataType.BIGINT;
                    break;
                }
                case BINARY:
                case BLOB:
                case MEDIUMBLOB:
                case TINYBLOB:
                case LONGBLOB:
                case VARBINARY: {
                    result =  HiveDataType.STRING;
                    break;
                }
                case TIME: {
                    result =  HiveDataType.STRING;
                    break;
                }

                case CHAR:
                case VARCHAR:
                case TINYTEXT:
                case TEXT:
                case LONGTEXT:
                case MEDIUMTEXT: {
                    result =  HiveDataType.STRING;
                    break;
                }

                case INT:
                case SMALLINT:
                case TINYINT:
                case INTEGER:
                case YEAR:
                case MEDIUMINT:
                case BIT: {
                    result =  HiveDataType.INT;
                    break;
                }

                /**
                 * hive 中PARQUET 目前无法很好的支持date 类型， 目前先转成string
                 */
                case DATE: {
                    result =  HiveDataType.STRING;
                    break;
                }

                case DATETIME:
                case TIMESTAMP: {
                    result =  HiveDataType.DATETIME;
                    break;
                }

                case DECIMAL:
                case NUMERIC: {
                    result = HiveDataType.DECIMAL;
                    break;
                }
                case DOUBLE:
                case REAL:
                case FLOAT: {
                    result =  HiveDataType.DOUBLE;
                    break;
                }
                case ENUM: {
                    result =  HiveDataType.STRING;
                    break;
                }
                default: {
                    result =  HiveDataType.STRING;
                    break;
                }
            }
        } while (false);
        return result;
    }
}
