package com.me.seraphcxl.etlscriptgenerator;

import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.Param;
import com.me.seraphcxl.column.HiveColumn;
import com.me.seraphcxl.utils.FileUtils;
import com.me.seraphcxl.utils.SqlUtils;
import java.util.ArrayList;
import org.apache.commons.collections4.CollectionUtils;

/**
 * @author xiaoliangchen
 */
public abstract class AbstractEtlScriptGenerator implements EtlScriptGenerator {
    @Override
    public int generateScript(JSONObject param) { return -1; }

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
                .append("FROM (\n")
                .append("SELECT\n")
                .append(SqlUtils.buildSelectColumnStr(null, Param.columns))
                ;
            if (CollectionUtils.isNotEmpty(Param.ods_mappingColumns)) {
                strBuilder.append(", ")
                    .append(SqlUtils.buildSelectMappingColumnStr(Param.ods_mappingColumns))
                    .append("\n")
                    ;
            }

            strBuilder.append(String.format("FROM %s.%s tblA\n", Param.odpsWorkSpaceName, Param.tableName_etlTableName))
                .append("WHERE 1 = 1\n")
                .append(String.format("AND %s IS NOT NULL\nAND %s = '000000000000'\n", HiveColumn.dw__src_id.getName(), HiveColumn.dw__plan_time.getName()))
                .append(") tbkB\n;\n\n")
            ;

            strBuilder.append(SqlUtils.sqlSeparator());
            if (FileUtils.saveEtlSplitToFile(Param.fileName_etl_fullMerge, strBuilder.toString()) != 0) {
                break;
            }
            result = 0;
        } while (false);
        return result;
    }

    protected int generateBlockChangeRecord() {
        int result = -1;
        do {
            StringBuilder strBuilder = new StringBuilder();

            ArrayList<HiveColumn> selectColumns = new ArrayList<>();
            selectColumns.addAll(Param.columns);
            selectColumns.addAll(Param.ods_mappingColumns);

            strBuilder.append(SqlUtils.sqlComment(Param.fileName_etl_changeRecord)).append("\n")
                .append(SqlUtils.sqlSeparator());

            strBuilder.append(SqlUtils.sqlSeparator())
                .append((String.format("INSERT OVERWRITE TABLE %s.%s PARTITION(%s)\n"
                    , Param.odpsWorkSpaceName, Param.tableName_odsChangeRecordTableName, HiveColumn.dw__plan_time.getName())))
                .append("SELECT\n")
                .append(SqlUtils.buildSelectColumnStr("tblB", selectColumns))
                .append(", ")
                .append(SqlUtils.getPartitionStrForSelect())
                .append("FROM (\n")
                .append("SELECT\n")
                .append(SqlUtils.buildSelectColumnStr("tblA", Param.columns));
            if (CollectionUtils.isNotEmpty(Param.ods_mappingColumns)) {
                strBuilder.append(", ")
                    .append(SqlUtils.buildSelectMappingColumnStr(Param.ods_mappingColumns));
            }
            strBuilder.append(", ")
                .append(SqlUtils.buildRowNumberStr("tblA", Param.pkColumns, Param.ods_mergeOrderBy))
                .append("\n")
                .append(String.format("FROM %s.%s tblA\n", Param.odpsWorkSpaceName, Param.tableName_etlTableName))
                .append("WHERE 1 = 1\n")
                .append(String.format("AND %s IS NOT NULL\n", HiveColumn.dw__src_id.getName()))
                .append(String.format("AND %s > to_char(dateadd(to_date(${bdp.system.cyctime}, 'yyyymmddhhmiss'), (-1 * %s), 'mi'), 'yyyymmddhhmi')\n"
                    , HiveColumn.dw__plan_time.getName()
                    , (Param.schedule_block_merge_schedule_minutes < Param.schedule_pull_schedule_minutes ? Param.schedule_block_merge_schedule_minutes + Param.schedule_pull_schedule_minutes : Param.schedule_block_merge_schedule_minutes)))
                .append(String.format("AND %s <= to_char(to_date(${bdp.system.cyctime}, 'yyyymmddhhmiss'), 'yyyymmddhhmi')\n", HiveColumn.dw__plan_time.getName()))
                .append(") tblB\n")
                .append("WHERE 1 = 1\n")
                .append("AND  tblB.dw_seq = 1\n")
                .append("-- LIMIT 999\n")
                .append(";\n\n");

            strBuilder.append(SqlUtils.sqlSeparator());
//            String tmpStr = strBuilder.toString();
            if (FileUtils.saveEtlSplitToFile(Param.fileName_etl_changeRecord, strBuilder.toString()) != 0) {
                break;
            }
            result = 0;
        } while (false);
        return result;
    }

    protected int generateBlockMerge() {
        int result = -1;
        do {
            StringBuilder strBuilder = new StringBuilder();

            ArrayList<HiveColumn> selectColumns = new ArrayList<>();
            selectColumns.addAll(Param.columns);
            selectColumns.addAll(Param.ods_mappingColumns);
            HiveColumn changePtCol = null;
            switch (Param.jobType.getOdsPartitionType()) {
                case Day:{
                    selectColumns.add(HiveColumn.ds);
                    changePtCol = HiveColumn.ds;
                    break;
                }
                case PT:{
                    selectColumns.add(Param.partitionKeyColumn);
                    changePtCol = Param.partitionKeyColumn;
                    break;
                }
                case OneMonthAndPT:{
                    selectColumns.add(HiveColumn.dm);
                    selectColumns.add(Param.partitionKeyColumn);
                    selectColumns.add(HiveColumn.dw__pt);
                    changePtCol = HiveColumn.dw__pt;
                    break;
                }
                default:
                    break;
            }

            strBuilder.append(SqlUtils.sqlComment(Param.fileName_etl_blockMerge)).append("\n")
                .append(SqlUtils.sqlSeparator());

            strBuilder.append(String.format("INSERT OVERWRITE TABLE %s.%s", Param.odpsWorkSpaceName, Param.tableName_odsTableName))
                .append(" ").append(SqlUtils.getPartitionStrForInsertSql()).append("\n")
                .append("SELECT\n")
                .append(SqlUtils.buildSelectColumnStr(null, selectColumns))
                .append("FROM (\n")
                .append("SELECT\n")
                .append(SqlUtils.buildSelectColumnStr(null, selectColumns))
                .append(", ")
                .append(SqlUtils.buildRowNumberStr(null, Param.pkColumns, Param.ods_mergeOrderBy))
                .append("\n")
                .append("FROM (\n")
                .append(String.format("SELECT\n"))
                .append(SqlUtils.buildSelectColumnStr("tblA", selectColumns))
                .append(String.format("FROM %s.%s %s\n", Param.odpsWorkSpaceName, Param.tableName_odsTableName, "tblA"))
                .append(String.format("WHERE 1 = 1\nAND %s IN(\n", changePtCol.getName()))
                .append(String.format("SELECT %s\nFROM %s.%s\nWHERE 1 = 1\n", changePtCol.getName()
                    , Param.odpsWorkSpaceName, Param.tableName_odsChangeRecordTableName))
                .append(String.format("AND %s > to_char(dateadd(to_date(${bdp.system.cyctime}, 'yyyymmddhhmiss'), (-1 * %s), 'mi'), 'yyyymmddhhmi')\n"
                    , HiveColumn.dw__plan_time.getName()
                    , (Param.schedule_block_merge_schedule_minutes < Param.schedule_pull_schedule_minutes ? Param.schedule_block_merge_schedule_minutes + Param.schedule_pull_schedule_minutes : Param.schedule_block_merge_schedule_minutes)))
                .append(String.format("AND %s <= to_char(to_date(${bdp.system.cyctime}, 'yyyymmddhhmiss'), 'yyyymmddhhmi')\n", HiveColumn.dw__plan_time.getName()))
                .append(String.format("GROUP BY %s\n)\n", changePtCol.getName()))
                .append(String.format("\nUNION ALL\n\nSELECT\n"))
                .append(SqlUtils.buildSelectColumnStr("tblB", selectColumns))
                .append(String.format("FROM %s.%s %s\nWHERE 1 = 1\n", Param.odpsWorkSpaceName, Param.tableName_odsChangeRecordTableName, "tblB"))
                .append(String.format("AND %s > to_char(dateadd(to_date(${bdp.system.cyctime}, 'yyyymmddhhmiss'), (-1 * %s), 'mi'), 'yyyymmddhhmi')\n"
                    , HiveColumn.dw__plan_time.getName()
                    , (Param.schedule_block_merge_schedule_minutes < Param.schedule_pull_schedule_minutes ? Param.schedule_block_merge_schedule_minutes + Param.schedule_pull_schedule_minutes : Param.schedule_block_merge_schedule_minutes)))
                .append(String.format("AND %s <= to_char(to_date(${bdp.system.cyctime}, 'yyyymmddhhmiss'), 'yyyymmddhhmi')\n", HiveColumn.dw__plan_time.getName()))
                .append(String.format(") %s\n", "tblC"))
                .append(String.format(") %s\n", "tblD"))
                .append(String.format("WHERE 1 = 1\nAND dw_seq = 1\n;\n\n"))
            ;

            strBuilder.append(SqlUtils.sqlSeparator());
//            String tmpStr = strBuilder.toString();
            if (FileUtils.saveEtlSplitToFile(Param.fileName_etl_blockMerge, strBuilder.toString()) != 0) {
                break;
            }
            result = 0;
        } while (false);
        return result;
    }
}
