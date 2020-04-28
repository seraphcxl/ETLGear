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
 * 批次 需要合并 ETL
 * @author xiaoliangchen
 */
public class BlockMergeEtlScriptGenerator implements EtlScriptGenerator {

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
            result = generateDataXBlockLoad();
            if (result < 0) {
                break;
            }
            result = generateFullMerge();
            if (result < 0) {
                break;
            }
            result = generateBlockMerge();
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

    protected int generateDataXBlockLoad() {
        int result = -1;
        do {
            DataXJsonBuilder.createDataXJson4BlockLoad();
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

            // ods ChangeRecordTable
            strBuilder.append(SqlUtils.sqlSeparator())
                .append(SqlUtils.buildDropTableStr(Param.odpsWorkSpaceName, Param.tableName_odsChangeRecordTableName))
                .append("\n");
            strBuilder.append(SqlUtils.sqlSeparator())
                .append(SqlUtils.buildCreateTableStr(
                    Param.odpsWorkSpaceName
                    , Param.tableName_odsChangeRecordTableName
                    , Param.columns
                    , Param.ods_mappingColumns
                    , String.format("%s %s ods 变动记录表", Param.bizName, Param.tableName)
                    , new ArrayList<>(Arrays.asList(HiveColumn.dw__plan_time))
                    , 45
                ))
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
            result = 0;
        } while (false);
        return result;
    }

    protected int generateBlockMerge() {
        int result = -1;
        do {
            result = 0;
        } while (false);
        return result;
    }
}
