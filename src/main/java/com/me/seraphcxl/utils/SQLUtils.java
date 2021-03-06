package com.me.seraphcxl.utils;

import com.me.seraphcxl.MergeOrderBy;
import com.me.seraphcxl.column.HiveColumn;
import com.me.seraphcxl.column.HiveMappingColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author xiaoliangchen
 */
public class SQLUtils {

    public static String sqlSeparator() {
        return "-- *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** --\n";
    }

    public static String sqlComment(String comment) {
        if (StringUtils.isBlank(comment)) {
            return "--";
        } else {
            return "-- " + comment;
        }
    }

    public static String buildCreateColumnStr(List<HiveColumn> columns) {
        String result = null;
        do {
            if (CollectionUtils.isEmpty(columns)) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            int idx = 0;
            for (HiveColumn col : columns) {
                if (idx > 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append(String.format("`%s` %s", col.getName(), col.getDataType()));
                if (StringUtils.isNoneBlank(col.getComment())) {
                    strBuilder.append(String.format(" COMMENT '%s' \n", col.getComment()));
                } else {
                    strBuilder.append("\n");
                }
                ++idx;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String buildSelectColumnStr(String tableName, List<HiveColumn> columns) {
        String result = null;
        do {
            if (CollectionUtils.isEmpty(columns)) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            int idx = 0;
            for (HiveColumn col : columns) {
                if (idx > 0) {
                    strBuilder.append(", ");
                }
                if (StringUtils.isNotBlank(tableName)) {
                    strBuilder.append(String.format("%s.", tableName));
                }
                strBuilder.append(String.format("%s AS %s\n", col.getName(), col.getName()));
                ++idx;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String buildSelectMappingColumnStr(List<HiveMappingColumn> columns) {
        String result = null;
        do {
            if (CollectionUtils.isEmpty(columns)) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            int idx = 0;
            for (HiveMappingColumn col : columns) {
                if (idx > 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append(String.format("%s AS %s\n", col.getMapping(), col.getName()));
                ++idx;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String buildMergeSelectColumnStr(
        String newTableName
        , String oldTableName
        , List<HiveColumn> columns
    ) {
        String result = null;
        do {
            if (CollectionUtils.isEmpty(columns)
                || StringUtils.isBlank(newTableName)
                || StringUtils.isBlank(oldTableName)
            ) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            int idx = 0;
            for (HiveColumn col : columns) {
                if (idx > 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append(String.format("CASE WHEN %s.%s IS NOT NULL THEN %s.%s ELSE %s.%s END AS %s\n"
                    , newTableName, col.getName(), newTableName, col.getName()
                    , oldTableName, col.getName(), col.getName()));
                ++idx;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String buildDropTableStr(String dbName, String tableName) {
        String result = null;
        do {
            if (StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName)) {
                break;
            }
            result = "DROP TABLE IF EXISTS " + dbName + "." + tableName + ";\n";
        } while (false);
        return result;
    }

    public static String buildCreateTableStr(
        String dbName
        , String tableName
        , List<HiveColumn> columns
        , List<HiveMappingColumn> mappingColumns
        , String tableComment
        , List<HiveColumn> partitions
        , int lifeCycle
    ) {
        String result = null;
        do {
            if (StringUtils.isBlank(dbName)
                || StringUtils.isBlank(tableName)
                || CollectionUtils.isEmpty(columns)
            ) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(String.format("CREATE TABLE IF NOT EXISTS %s.%s (\n", dbName, tableName));
            strBuilder.append(buildCreateColumnStr(columns));
            if (CollectionUtils.isNotEmpty(mappingColumns)) {
                strBuilder.append(", ")
                    .append(buildCreateColumnStr((List)mappingColumns));
            }
            if (StringUtils.isNotBlank(tableComment)) {
                strBuilder.append(String.format(") COMMENT '%s'\n", tableComment));
            } else {
                strBuilder.append(")\n");
            }
            if (CollectionUtils.isNotEmpty(partitions)) {
                strBuilder.append("PARTITIONED BY (\n");
                strBuilder.append(buildCreateColumnStr(partitions));
                strBuilder.append(")\n");
            }
            if (lifeCycle > 0) {
                strBuilder.append(String.format("LIFECYCLE %d\n", lifeCycle));
            }
            strBuilder.append(";\n");
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String buildRowNumberStr(
        String tableName,
        List<HiveColumn> pk,
        List<MergeOrderBy> mergeOrderBy
    ) {
        String result = null;
        do {
            if (CollectionUtils.isEmpty(pk) || CollectionUtils.isEmpty(mergeOrderBy)) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder("row_number() over (partition by ");
            int idx = 0;
            for (HiveColumn col : pk) {
                if (idx > 0) {
                    strBuilder.append(", ");
                }
                if (StringUtils.isNotBlank(tableName)) {
                    strBuilder.append(tableName + ".");
                }
                strBuilder.append(col.getName());
                ++idx;
            }
            strBuilder.append(" ORDER BY ");
            idx = 0;
            for (MergeOrderBy orderBy : mergeOrderBy) {
                if (idx > 0) {
                    strBuilder.append(", ");
                }
                if (StringUtils.isNotBlank(tableName)) {
                    strBuilder.append(tableName + ".");
                }
                strBuilder.append(orderBy.getColumn() + " " + orderBy.getOrderBy());
                ++idx;
            }
            strBuilder.append(") AS dw_seq");
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String buildJoinOnStr(
        String newTableName
        , String oldTableName
        , List<HiveColumn> columns
    ) {
        String result = null;
        do {
            if (CollectionUtils.isEmpty(columns)) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            int idx = 0;
            for (HiveColumn col : columns) {
                if (idx > 0) {
                    strBuilder.append("AND ");
                }
                strBuilder.append(String.format("%s.%s = %s.%s\n"
                    , newTableName, col.getName(), oldTableName, col.getName()));
                ++idx;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static List<HiveColumn> getColumnsWithoutMappingColumns(
        List<HiveColumn> columns
        , List<HiveMappingColumn> mappingColumns
    ) {
        List<HiveColumn> result = null;
        do {
            result = new ArrayList<>(Arrays.asList(new HiveColumn[columns.size()]));
            Collections.copy(result, columns);

            ArrayList<String> mappingColumnNames = new ArrayList<>(mappingColumns.size());
            for (HiveMappingColumn col : mappingColumns) {
                mappingColumnNames.add(col.getName());
            }

            Iterator<HiveColumn> it = result.iterator();
            while (it.hasNext()) {
                HiveColumn col = it.next();
                if (mappingColumnNames.indexOf(col.getName()) != -1) {
                    it.remove();
                }
            }
        } while (false);
        return result;
    }
}
