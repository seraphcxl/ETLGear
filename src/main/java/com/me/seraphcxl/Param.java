package com.me.seraphcxl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.column.HiveColumn;
import com.me.seraphcxl.column.HiveColumnBuilder;
import com.me.seraphcxl.column.HiveMappingColumn;
import com.me.seraphcxl.utils.MD5Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

/**
 * @author xiaoliangchen
 */
public class Param {
    // 参数集合的MD5
    public static String paramMD5 = "";
    // ETL 任务的类型
    public static EtlJobType jobType = null;
    // ODPS 工作空间的名字，也就是Hive的库名
    public static String odpsWorkSpaceName = "";
    // 业务DB名
    public static String bizName = "";
    // 业务Table名
    public static String tableName = "";

    // 业务Table的字段
    public static List<HiveColumn> columns = null;
    // 业务Table的主键字段
    public static List<HiveColumn> pkColumns = null;
    // 数据源表里代表 create_time 含义的字段
    public static HiveColumn createTimeColumn = null;
    // 数据源表里代表 update_time 含义的字段
    public static HiveColumn updateTimeColumn = null;
    // 数据源表里代表 splitPk 含义的字段，DataX用
    public static HiveColumn splitPkColumn = null;
    // 对于用 string 做分区的ODS表，用来标注源表里那个字段作为分区字段
    public static HiveColumn partitionKeyColumn = null;
    /** Source
     *
     */
    public static String source_tableCreateSQL = null;
    public static List<String> source_pkColumnNames = null;
    public static String source_createTimeColumnName = null;
    public static String source_updateTimeColumnName = null;
    public static String source_splitPkColumnName = null;
    // DataX 用的数据源，应该是 ODPS 的实例的数据库
    public static List<String> source_dataSource = null;
    // 数据源表名
    public static String source_tableName = "";
    // DataX用的自定义Where
    public static String source_where = "";
    // DataX用的自定义查询语句
    public static String source_querySql = "";

    /** ETL
     *
     */
    // ETL表用的附加分区字段
    public static String etl_partition = "";
    // ETL表的写入数据库
    public static String etl_odpsDataSource = "";

    /** ODS
     *
     */
    // ODS表 生成时对于特定字段做 Mapping 处理，相应的信息
    public static List<HiveMappingColumn> ods_mappingColumns = null;
    // ODS表 合并时用来在PK窗口上做排序的信息
    public static List<MergeOrderBy> ods_mergeOrderBy = null;

    /** schedule
     *
     */
    public static int schedule_pull_schedule_minutes = 60;
    public static int schedule_block_merge_schedule_minutes = 60 * 24;

    /** file name
     *
     */
    public static String fileName_dataX_fullLoad = "";
    public static String fileName_dataX_blockLoad = "";
    public static String fileName_etl_createTable = "";
    public static String fileName_etl_ChangeRecord = "";
    public static String fileName_etl_fullMerge = "";
    public static String fileName_etl_blockMerge = "";

    /** table name
     *
     */
    public static String tableName_etlTableName = "";
    public static String tableName_odsTableName = "";
    public static String tableName_odsChangeRecordTableName = "";

    public static int parseJobParam(JSONObject param) {
        int result = -1;
        do {
            JSONObject source = param.getJSONObject("source");
            JSONObject etl = param.getJSONObject("etl");
            JSONObject ods = param.getJSONObject("ods");
            JSONObject schedule = param.getJSONObject("schedule");

            JSONObject jobTypeJsonObj = param.getJSONObject("ETLJobType");
            jobType = JSONObject.parseObject(jobTypeJsonObj.toJSONString(), EtlJobType.class);
            odpsWorkSpaceName = param.getString("odpsWorkSpaceName");
            bizName = param.getString("bizName");
            tableName = param.getString("tableName");

            /** Source
             *
             */
            source_tableCreateSQL = source.getString("tableCreateSQL");
            JSONArray pkColumnNamesArray = source.getJSONArray("pkColumnNames");
            if (CollectionUtils.isNotEmpty(pkColumnNamesArray)) {
                source_pkColumnNames = JSONObject.parseArray(pkColumnNamesArray.toJSONString(), String.class);
            }
            source_createTimeColumnName = source.getString("createTimeColumnName");
            source_updateTimeColumnName = source.getString("updateTimeColumnName");
            source_splitPkColumnName = source.getString("splitPkColumnName");
            JSONArray srcDataSourceArray = source.getJSONArray("dataSource");
            if (CollectionUtils.isNotEmpty(srcDataSourceArray)) {
                source_dataSource = JSONObject.parseArray(srcDataSourceArray.toJSONString(), String.class);
            }
            source_tableName = source.getString("dataSourceTableName");
            source_where = source.getString("where");
            source_querySql = source.getString("querySql");

            /** etl
             *
             */
            etl_partition = etl.getString("partition");
            etl_odpsDataSource = etl.getString("odpsDataSource");

            /** ods
             *
             */
            JSONArray odsColumnMappingArray = ods.getJSONArray("mappingColumns");
            if (CollectionUtils.isNotEmpty(odsColumnMappingArray)) {
                ods_mappingColumns = JSONObject.parseArray(odsColumnMappingArray.toJSONString(), HiveMappingColumn.class);
            } else {
                ods_mappingColumns = new ArrayList<>(Arrays.asList());
            }
            JSONArray odsMergeOrderByArray = ods.getJSONArray("mergeOrderBy");
            if (CollectionUtils.isNotEmpty(odsMergeOrderByArray)) {
                ods_mergeOrderBy = JSONObject.parseArray(odsMergeOrderByArray.toJSONString(), MergeOrderBy.class);
            } else {
                ods_mergeOrderBy = new ArrayList<>(Arrays.asList());
            }

            /** schedule
             *
             */
            schedule_pull_schedule_minutes = schedule.getIntValue("pull_schedule_minutes");
            schedule_block_merge_schedule_minutes = schedule.getIntValue("block_merge_schedule_minutes");

            paramMD5 = MD5Utils.stringToMD5(param.toJSONString());

            columns = HiveColumnBuilder.parseColumns(source_tableCreateSQL
                , source_pkColumnNames , source_createTimeColumnName
                , source_updateTimeColumnName, source_splitPkColumnName
            );

            ArrayList<HiveColumn> tmpList = new ArrayList<>(columns);
            tmpList.addAll(ods_mappingColumns);
            pkColumns = new ArrayList<>();
            for (HiveColumn col : tmpList) {
                if (col.isPk()) {
                    pkColumns.add(col);
                }
                if (col.isSplitPk()) {
                    splitPkColumn = col;
                }
                if (col.isCreateTime()) {
                    createTimeColumn = col;
                }
                if (col.isUpdateTime()) {
                    updateTimeColumn = col;
                }
                if (col.isPartitionKey()) {
                    partitionKeyColumn = col;
                }
            }

            tableName_etlTableName = "t_etl_" + bizName + "_" + tableName + "_h";
            tableName_odsTableName = "t_ods_" + bizName + "_" + tableName;
            tableName_odsChangeRecordTableName = tableName_odsTableName + "_change_record";

            fileName_dataX_fullLoad = "etl_input_" + bizName + "_" + tableName + "_full_load.json";
            fileName_dataX_blockLoad = "etl_input_" + bizName + "_" + tableName + "_block_load.json";
            fileName_etl_createTable = tableName_odsTableName + "_create_table.hql";
            fileName_etl_ChangeRecord = tableName_odsTableName + "_change_record.hql";
            fileName_etl_fullMerge = tableName_odsTableName + "_full_merge.hql";
            fileName_etl_blockMerge = tableName_odsTableName + "_block_merge.hql";

            result = 0;
        } while (false);
        return result;
    }

    /**
     * 检查json参数文件
     * @param param
     * @return
     */
    public static Boolean checkJSONParam(JSONObject param) {
        Boolean result = false;
        do {
            if (param == null) {
                break;
            }
            if (StringUtils.isBlank(paramMD5)) {
                parseJobParam(param);
                if (StringUtils.isBlank(paramMD5)) {
                    break;
                }
            } else if (!paramMD5.equals(MD5Utils.stringToMD5(param.toJSONString()))) {
                break;
            }

            Assert.assertTrue("StringUtils.isNotBlank(Param.odpsWorkSpaceName)"
                , StringUtils.isNotBlank(Param.odpsWorkSpaceName));
            Assert.assertTrue("StringUtils.isNotBlank(Param.bizName)"
                , StringUtils.isNotBlank(Param.bizName));
            Assert.assertTrue("StringUtils.isNotBlank(Param.tableName)"
                , StringUtils.isNotBlank(Param.tableName));
            Assert.assertTrue("ODPS dataSource list should less than 150"
                , Param.source_dataSource.size() <= 150);
            Assert.assertTrue("schedule_pull_schedule_minutes > 0"
                , Param.schedule_pull_schedule_minutes > 0);
            Assert.assertTrue("allMerge_schedule_minutes > pull_schedule_minutes"
                , Param.schedule_block_merge_schedule_minutes > Param.schedule_pull_schedule_minutes);
            result = true;
        } while (false);
        return result;
    }
}
