package com.me.seraphcxl.etlscriptgenerator;

import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.DataXJsonBuilder;
import com.me.seraphcxl.Param;
import com.me.seraphcxl.column.HiveColumn;
import com.me.seraphcxl.utils.FileUtils;
import com.me.seraphcxl.utils.SqlUtils;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * 全量 ETL
 * @author xiaoliangchen
 */
public class FullEtlScriptGenerator implements EtlScriptGenerator {

    /**
     * 只有 full_load.json 和 full_merge.hql
     * @param param
     * @return
     */
    @Override
    public int generateScript(JSONObject param) {
        int result = -1;
        do {
            result = generateCreateTable();
            if (result < 0) {
                break;
            }
            result = generateDataXFullLoad();
            if (result < 0) {
                break;
            }
            result = generateFullMerge();
        } while (false);
        return result;
    }

    protected int generateDataXFullLoad() {
        int result = -1;
        do {
            DataXJsonBuilder.createDataXJson4FullLoad();
            result = 0;
        } while (false);
        return result;
    }

    protected int generateCreateTable() {
        int result = -1;
        do {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(SqlUtils.sqlComment(Param.fileName_etl_createTable)).append("\n");
            // etlTable
            strBuilder.append(SqlUtils.sqlSeparator())
                .append(SqlUtils.buildDropTableStr(Param.odpsWorkSpaceName, Param.tableName_etlTableName))
                .append("\n");
            strBuilder.append(SqlUtils.sqlSeparator())
                .append(SqlUtils.buildCreateTableStr(
                    Param.odpsWorkSpaceName
                    , Param.tableName_etlTableName
                    , Param.columns
                    , new ArrayList<>()
                    , String.format("%s %s etl 增量同步表", Param.bizName, Param.tableName)
                    , new ArrayList<>(Arrays.asList(HiveColumn.dw__src_id, HiveColumn.dw__plan_time))
                    , 45
                ))
                .append("\n");

            // odsTable
            strBuilder.append(SqlUtils.sqlSeparator())
                .append(SqlUtils.buildCreateODSTableStrWithPatitionType())
                .append("\n");

            strBuilder.append(SqlUtils.sqlSeparator());
            if (FileUtils.saveETLSplitToFile(Param.fileName_etl_createTable, strBuilder.toString()) != 0) {
                break;
            }
            result = 0;
        } while (false);
        return result;
    }

    protected int generateFullMerge() {
        int result = -1;
        do {
            StringBuilder strBuilder = new StringBuilder();

            ArrayList<HiveColumn> selectColumns = new ArrayList<>();
            selectColumns.addAll(Param.columns);
            selectColumns.addAll(Param.ods_mappingColumns);

            strBuilder.append(SqlUtils.sqlComment(Param.fileName_etl_fullMerge)).append("\n")
                .append(SqlUtils.sqlSeparator());

            strBuilder.append(String.format("INSERT OVERWRITE TABLE %s.%s", Param.odpsWorkSpaceName, Param.tableName_odsTableName))
                .append(" ").append(SqlUtils.getPartitionStrForInsertSql()).append("\n")
                .append("SELECT\n")
                .append(SqlUtils.buildSelectColumnStr(null, selectColumns))
                .append(", ")
                .append(SqlUtils.getPartitionStrForSelect())
                .append(String.format("FROM %s.%s tblA\n", Param.odpsWorkSpaceName, Param.tableName_etlTableName))
                .append("WHERE 1 = 1\n")
                .append(String.format("AND %s IS NOT NULL\nAND %s = '000000000000'\n", HiveColumn.dw__src_id.getName(), HiveColumn.dw__plan_time.getName()))
                .append(";\n\n")
            ;

            if (FileUtils.saveETLSplitToFile(Param.fileName_etl_fullMerge, strBuilder.toString()) != 0) {
                break;
            }
            result = 0;
        } while (false);
        return result;
    }
}
