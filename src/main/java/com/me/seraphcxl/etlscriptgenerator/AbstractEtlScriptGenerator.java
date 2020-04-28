package com.me.seraphcxl.etlscriptgenerator;

import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.Param;
import com.me.seraphcxl.column.HiveColumn;
import com.me.seraphcxl.utils.FileUtils;
import com.me.seraphcxl.utils.SqlUtils;
import java.util.ArrayList;

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
