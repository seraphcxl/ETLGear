package com.me.seraphcxl.column;

import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.datatype.DataTypeConvertor;
import com.me.seraphcxl.datatype.HiveDataType;
import com.me.seraphcxl.datatype.MySqlDataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * @author xiaoliangchen
 */
public class HiveColumnBuilder {
    public static List<HiveColumn> parseColumns(
        String tableCreateSQL
        , List<String> pkColumnNames
        , String createTimeColumnName
        , String updateTimeColumnName
        , String splitPkColumnName
    ) {
        List<HiveColumn> result = new ArrayList<>(Arrays.asList());
        do {
            String startFlag = "(\n";
            String endFlag = "\n  PRIMARY KEY";
            int startIdx = tableCreateSQL.indexOf(startFlag);
            int endIdx = tableCreateSQL.indexOf(endFlag);
            if (endIdx == -1) {
                System.out.println("没有找到数据库表的主键！！！");
                break;
            }

            String tmp = tableCreateSQL.substring(startIdx + startFlag.length(), endIdx).trim();

            String[] columns = tmp.split("\n");
            ArrayList<HiveColumn> hiveColumns = new ArrayList<>(Arrays.asList());
            for (int i = 0; i < columns.length; ++i) {
                String[] columnElements = columns[i].split(" ");
                String srcName = null;
                Boolean hasDataType = false;
                HiveDataType dataType = HiveDataType.STRING;
                String comment = "源数据无备注";
                Boolean hasComment = false;
                for (int j = 0; j < columnElements.length; ++j) {
                    String element = columnElements[j].trim()
                        .replace("`", "").replace("'", "")
                        .replace(",", "");
                    if (StringUtils.isNotBlank(element)) {
                        if (StringUtils.isBlank(srcName)) {
                            srcName = element;
                        } else if (!hasDataType) {
                            dataType = DataTypeConvertor.convMySql2Hive(MySqlDataType.toTypeWithStr(element));
                            hasDataType = true;
                        } else {
                            if (element.toLowerCase().equals("comment")) {
                                hasComment = true;
                            } else if (hasComment) {
                                String tmpStr = columns[i].trim().toLowerCase()
                                    .replace("`", "").replace("'", "")
                                    .replace(",", "");
                                comment = tmpStr.substring((tmpStr.indexOf("comment") + "comment".length())).trim();
                            }
                        }
                    }
                }

                Boolean isPk = false;
                for (String pkColumnName : pkColumnNames) {
                    if (pkColumnName.equals(srcName)) {
                        isPk = true;
                        break;
                    }
                }

                Boolean isCreateTime = false;
                if (createTimeColumnName != null
                    && createTimeColumnName.equals(srcName)
                ) {
                    isCreateTime = true;
                }

                Boolean isUpdateTime = false;
                if (updateTimeColumnName != null
                    && updateTimeColumnName.equals(srcName)
                ) {
                    isUpdateTime = true;
                }

                Boolean isSplitPk = false;
                if (splitPkColumnName != null
                    && splitPkColumnName.equals(srcName)
                ) {
                    isSplitPk = true;
                }

                HiveColumn hiveCol = new HiveColumn(srcName, srcName, dataType, comment, isPk, isSplitPk, isCreateTime, isUpdateTime, false);
                hiveColumns.add(hiveCol);
            }
            result = hiveColumns;
            int z = 0;
        } while (false);
        return result;
    }
}
