package com.me.seraphcxl.utils;

import com.me.seraphcxl.MergeOrderBy;
import com.me.seraphcxl.Param;
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
public class SqlUtils {

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

    public static String buildCreateODSTableStrWithPartitionType() {
        String result = null;
        do {
            ArrayList<HiveColumn> selectColumns = new ArrayList<>();
            selectColumns.addAll(Param.columns);
            selectColumns.addAll(Param.ods_mappingColumns);

            List<HiveColumn> partitions = null;
            switch (Param.jobType.getOdsPartitionType()) {
                case Day:{
                    partitions = new ArrayList<>(Arrays.asList(HiveColumn.ds));
                    break;
                }
                case PT:{
                    partitions = new ArrayList<>(Arrays.asList(Param.partitionKeyColumn));
                    break;
                }
                case OneMonthAndPT:{
                    partitions = new ArrayList<>(Arrays.asList(HiveColumn.dm
                        , Param.partitionKeyColumn, HiveColumn.dw__pt));
                    break;
                }
                default:
                    break;
            }
            if (CollectionUtils.isEmpty(partitions)) {
                break;
            }
            result = SqlUtils.buildCreateTableStr(
                Param.odpsWorkSpaceName
                , Param.tableName_odsTableName
                , selectColumns
                , String.format("%s %s ods 表", Param.bizName, Param.tableName)
                , partitions
                , -1
            );
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

    public static String getDSPartitionStr(HiveColumn col) {
        String result = null;
        do {
            if (col == null || !col.isCreateTime()) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            switch (col.getDataType()) {
                case DATETIME:{
                    strBuilder.append(String.format("CASE WHEN %s IS NULL OR CAST(TO_CHAR(%s, 'yyyymmdd') AS BIGINT) <= 19700101 THEN 19700101\n"
                            + "WHEN CAST(TO_CHAR(%s, 'yyyymmdd') AS BIGINT) <= 20161231 THEN 20161231\n"
                            + "WHEN CAST(TO_CHAR(%s, 'yyyymmdd') AS BIGINT) >= 20300101 THEN 20300101\n"
                            + "ELSE CAST(TO_CHAR(%s, 'yyyymmdd') AS BIGINT) END"
                        , col.getName(), col.getName(), col.getName(), col.getName()
                        , col.getName()));
                    break;
                }
                case TIMESTAMP:{
                    strBuilder.append(String.format("CASE WHEN %s IS NULL OR CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymmdd') AS BIGINT) <= 19700101 THEN 19700101\n"
                            + "WHEN CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymmdd') AS BIGINT) <= 20161231 THEN 20161231\n"
                            + "WHEN CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymmdd') AS BIGINT) >= 20300101 THEN 20300101\n"
                            + "ELSE CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymmdd') AS BIGINT) END"
                        , col.getName(), col.getName(), col.getName(), col.getName()
                        , col.getName()));
                    break;
                }
                default:
                    break;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String getDMPartitionStr(HiveColumn col) {
        String result = null;
        do {
            if (col == null || !col.isCreateTime()) {
                break;
            }
            StringBuilder strBuilder = new StringBuilder();
            switch (col.getDataType()) {
                case DATETIME:{
                    strBuilder.append(String.format("CASE WHEN %s IS NULL OR CAST(TO_CHAR(%s, 'yyyymm') AS BIGINT) <= 197001 THEN 197001\n"
                            + "WHEN CAST(TO_CHAR(%s, 'yyyymm') AS BIGINT) <= 201612 THEN 201612\n"
                            + "WHEN CAST(TO_CHAR(%s, 'yyyymm') AS BIGINT) >= 203001 THEN 203001\n"
                            + "ELSE CAST(TO_CHAR(%s, 'yyyymm') AS BIGINT) END"
                        , col.getName(), col.getName(), col.getName(), col.getName()
                        , col.getName()));
                    break;
                }
                case TIMESTAMP:{
                    strBuilder.append(String.format("CASE WHEN %s IS NULL OR CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymm') AS BIGINT) <= 197001 THEN 197001\n"
                            + "WHEN CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymm') AS BIGINT) <= 201612 THEN 201612\n"
                            + "WHEN CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymm') AS BIGINT) >= 203001 THEN 203001\n"
                            + "ELSE CAST(TO_CHAR(FROM_UNIXTIME(%s), 'yyyymm') AS BIGINT) END"
                        , col.getName(), col.getName(), col.getName(), col.getName()
                        , col.getName()));
                    break;
                }
                default:
                    break;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String getPartitionStrForInsertSql() {
        String result = null;
        do {
            StringBuilder strBuilder = new StringBuilder();
            switch (Param.jobType.getOdsPartitionType()) {
                case Day:{
                    strBuilder.append(String.format("PARTITION(%s)", HiveColumn.ds.getName()));
                    break;
                }
                case PT:{
                    strBuilder.append(String.format("PARTITION(%s)", Param.partitionKeyColumn.getName()));
                    break;
                }
                case OneMonthAndPT:{
                    strBuilder.append(String.format("PARTITION(%s, %s, %s)", HiveColumn.dm.getName()
                        , Param.partitionKeyColumn.getName(), HiveColumn.dw__pt.getName()));
                    break;
                }
                default:
                    break;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }

    public static String getPartitionStrForSelect() {
        String result = null;
        do {
            StringBuilder strBuilder = new StringBuilder();
            switch (Param.jobType.getOdsPartitionType()) {
                case Day:{
                    strBuilder.append(SqlUtils.getDSPartitionStr(Param.createTimeColumn))
                        .append(String.format(" AS %s\n", HiveColumn.ds.getName()))
                    ;
                    break;
                }
                case PT:{
                    strBuilder.append(SqlUtils.buildSelectMappingColumnStr(new ArrayList(
                        Collections.singletonList(Param.partitionKeyColumn))));
                    break;
                }
                case OneMonthAndPT:{
                    strBuilder.append(SqlUtils.getDMPartitionStr(Param.createTimeColumn))
                        .append(String.format(" AS %s\n", HiveColumn.dm.getName()))
                        .append(", ")
                        .append(SqlUtils.buildSelectMappingColumnStr(new ArrayList(
                            Collections.singletonList(Param.partitionKeyColumn))))
                        .append(String.format(", CONCAT(%s, '__', %s) AS %s\n", SqlUtils.getDMPartitionStr(Param.createTimeColumn)
                            , ((HiveMappingColumn)Param.partitionKeyColumn).getMapping(), HiveColumn.dw__pt.getName()))
                    ;
                    break;
                }
                default:
                    break;
            }
            result = strBuilder.toString();
        } while (false);
        return result;
    }
}
