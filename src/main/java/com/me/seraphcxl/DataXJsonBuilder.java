package com.me.seraphcxl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.column.HiveColumn;
import com.me.seraphcxl.utils.FileUtils;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class DataXJsonBuilder {

    /**
     * DataX Task 的 类型
     */
    public enum DataXTaskType {
        // 初始化任务，或者 重跑某个时间区间数据，或者 添加数据源后跑一下
        FullLoad
        // 增量导入任务
        , BlockLoad
    }

    public static String buildETLColumnsJSONStr(List<HiveColumn> columns) {
        String result = null;
        do {
            if (CollectionUtils.isEmpty(columns)) {
                break;
            }

            JSONArray jsonArray = new JSONArray();
            for (HiveColumn col : columns) {
                JSONObject jsonObj = new JSONObject();
                jsonObj.fluentPut("name", col.getName());
                jsonObj.fluentPut("srcName", col.getSrcName());
                jsonObj.fluentPut("dataType", col.getDataType());
                jsonObj.fluentPut("comment", col.getComment());

                if (col.isPk()) {
                    jsonObj.fluentPut("pk", true);
                }

                if (col.isSplitPk()) {
                    jsonObj.fluentPut("splitPk", true);
                }

                if (col.isCreateTime()) {
                    jsonObj.fluentPut("createTime", true);
                }

                if (col.isUpdateTime()) {
                    jsonObj.fluentPut("updateTime", true);
                }

                jsonArray.add(jsonObj);
            }

            JSONObject jsonObject = new JSONObject();
            jsonObject.fluentPut("column", jsonArray);

            result = jsonObject.toJSONString();
        } while (false);
        return result;
    }

    private static JSONArray buildColumnJSONObjList(List<HiveColumn> columns, boolean useColumnSrcName) {
        JSONArray result = new JSONArray();
        do {
            if (CollectionUtils.isEmpty(columns)) {
                break;
            }
            for (HiveColumn col : columns) {
                String colName = col.getName();
                if (useColumnSrcName) {
                    colName = col.getSrcName();
                }
                result.add(colName);
            }
        } while (false);
        return result;
    }

    private static JSONArray buildDataSourceJSONObjList(List<String> dataSrc, String tableName) {
        JSONArray result = new JSONArray();
        do {
            if (CollectionUtils.isEmpty(dataSrc) || StringUtils.isBlank(tableName)) {
                break;
            }
            for (String dataSrcName : dataSrc) {
                JSONObject jsonObj = new JSONObject();
                jsonObj.fluentPut("datasource", dataSrcName);
                JSONArray tables = new JSONArray();
                tables.add(tableName);
                jsonObj.fluentPut("table", tables);
                result.add(jsonObj);
            }
        } while (false);
        return result;
    }

    private static JSONObject buildMySQLReaderJSONObj(DataXTaskType taskType) {
        JSONObject result = new JSONObject();
        do {
            result.fluentPut("stepType", "mysql")
                .fluentPut("name", "Reader")
                .fluentPut("category", "reader");
            JSONObject parameter = new JSONObject();
            parameter.fluentPut("encoding", "UTF-8");
            if (Param.splitPkColumn != null) {
                parameter.fluentPut("splitPk", Param.splitPkColumn.getSrcName());
            }
            parameter.fluentPut("column", buildColumnJSONObjList(Param.columns, true))
                .fluentPut("connection", buildDataSourceJSONObjList(Param.source_dataSource, Param.source_tableName));
            switch (taskType) {
                case FullLoad: {
                    if (StringUtils.isBlank(Param.source_where)
                        && Param.createTimeColumn != null
                    ) {
                        switch (Param.createTimeColumn.getDataType()) {
                            case DATETIME: {
                                parameter.fluentPut("where", String.format("%s >= date_format(${start_ds}, '%%Y%%m%%d%%H%%i%%s') "
                                        + "AND %s < date_format(${end_ds}, '%%Y%%m%%d%%H%%i%%s')"
                                    , Param.createTimeColumn.getSrcName(), Param.createTimeColumn.getSrcName()));
                                break;
                            }
                            case TIMESTAMP: {
                                parameter.fluentPut("where", String.format("%s >= UNIX_TIMESTAMP(date_format(${start_ds}, '%%Y%%m%%d%%H%%i%%s')) "
                                        + "AND %s < UNIX_TIMESTAMP(date_format(${end_ds}, '%%Y%%m%%d%%H%%i%%s'))"
                                    , Param.createTimeColumn.getSrcName(), Param.createTimeColumn.getSrcName()));
                                break;
                            }
                            default:
                                break;
                        }
                    } else {
                        parameter.fluentPut("where", "");
                    }
                    break;
                }
                case BlockLoad: {
                    if (StringUtils.isBlank(Param.source_where)
                        && Param.updateTimeColumn != null
                    ) {
                        switch (Param.updateTimeColumn.getDataType()) {
                            case DATETIME: {
                                parameter.fluentPut("where", String.format("%s >= date_format(date_add(str_to_date(${bdp.system.cyctime}, '%%Y%%m%%d%%H%%i%%s'), INTERVAL (-1 * %s) MINUTE), '%%Y-%%m-%%d %%H:%%i:%%s') "
                                        + "AND %s < date_format(str_to_date(${bdp.system.cyctime}, '%%Y%%m%%d%%H%%i%%s'), '%%Y-%%m-%%d %%H:%%i:%%s')"
                                    , Param.updateTimeColumn.getSrcName(), Param.schedule_pull_schedule_minutes, Param.updateTimeColumn.getSrcName()));
                                break;
                            }
                            case TIMESTAMP: {
                                parameter.fluentPut("where", String.format("%s >= UNIX_TIMESTAMP(date_format(date_add(str_to_date(${bdp.system.cyctime}, '%%Y%%m%%d%%H%%i%%s'), INTERVAL (-1 * %s) MINUTE), '%%Y-%%m-%%d %%H:%%i:%%s')) "
                                        + "AND %s < UNIX_TIMESTAMP(date_format(str_to_date(${bdp.system.cyctime}, '%%Y%%m%%d%%H%%i%%s'), '%%Y-%%m-%%d %%H:%%i:%%s'))"
                                    , Param.updateTimeColumn.getSrcName(), Param.schedule_pull_schedule_minutes, Param.updateTimeColumn.getSrcName()));
                                break;
                            }
                            default:
                                break;
                        }
                    } else {
                        parameter.fluentPut("where", Param.source_where);
                    }
                    break;
                }
                default:
            }

            result.fluentPut("parameter", parameter);
        } while (false);
        return result;
    }

    private static JSONObject buildODPSWriterJSONObj() {
        JSONObject result = new JSONObject();
        do {
            result.fluentPut("stepType", "odps")
                .fluentPut("name", "Writer")
                .fluentPut("category", "writer");
            JSONObject parameter = new JSONObject();
            parameter.fluentPut("truncate", "true")
                .fluentPut("compress", "true")
                .fluentPut("emptyAsNull", "false")
                .fluentPut("partition", (Param.etl_partition + ", dw_plan_time = ${dw_plan_time}"))
                .fluentPut("datasource", Param.etl_odpsDataSource)
                .fluentPut("table", Param.tableName_etlTableName)
                .fluentPut("column", buildColumnJSONObjList(Param.columns, false));
            result.fluentPut("parameter", parameter);
        } while (false);
        return result;
    }

    private static JSONObject buildDefaultJSONRootObj() {
        JSONObject result = new JSONObject();
        do {
            result.fluentPut("type", "job")
                .fluentPut("version", "2.0");

            JSONObject hop = new JSONObject();
            hop.fluentPut("from", "Reader")
                .fluentPut("to", "Writer");
            JSONArray hops = new JSONArray();
            hops.add(hop);
            JSONObject order = new JSONObject();
            order.fluentPut("hops", hops);

            JSONObject errorLimit = new JSONObject();
            errorLimit.fluentPut("record", "0");
            JSONObject speed = new JSONObject();
            speed.fluentPut("throttle", "false")
                .fluentPut("concurrent", "8");
            JSONObject setting = new JSONObject();
            setting.fluentPut("errorLimit", errorLimit)
                .fluentPut("speed", speed);

            result.fluentPut("order", order)
                .fluentPut("setting", setting);
        } while (false);
        return result;
    }

    public static String createDataXJson4FullLoad(JSONObject param) {
        String result = null;
        do {
            JSONObject root = buildDefaultJSONRootObj();
            root.fluentPut("HupunComment", "dw_src_id=1 dw_plan_time=000000000000 start_ds=00000000000000 end_ds=99991231235959");

            JSONArray steps = new JSONArray();
            steps.fluentAdd(buildMySQLReaderJSONObj(DataXTaskType.FullLoad))
                .fluentAdd(buildODPSWriterJSONObj());

            root.fluentPut("steps", steps);

            result = root.toJSONString();

            if (FileUtils.saveDataXJsonToFile(Param.fileName_dataX_fullLoad, result) != 0) {
                break;
            }
        } while (false);
        return result;
    }

    public static String createDataXJson4BlockLoad(JSONObject param) {
        String result = null;
        do {
            JSONObject root = buildDefaultJSONRootObj();
            root.fluentPut("HupunComment", "dw_src_id=1 dw_plan_time=$[yyyymmddhh24mi]");

            JSONArray steps = new JSONArray();
            steps.fluentAdd(buildMySQLReaderJSONObj(DataXTaskType.BlockLoad))
                .fluentAdd(buildODPSWriterJSONObj());

            root.fluentPut("steps", steps);

            result = root.toJSONString();

            if (FileUtils.saveDataXJsonToFile(Param.fileName_dataX_blockLoad, result) != 0) {
                break;
            }
        } while (false);
        return result;
    }
}
