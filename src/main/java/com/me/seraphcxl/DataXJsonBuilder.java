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

    private static JSONArray buildColumnJsonObjList(List<HiveColumn> columns, boolean useColumnSrcName) {
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

    private static JSONArray buildDataSourceJsonObjList(List<String> dataSrc, String tableName) {
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

    private static JSONObject buildMysqlReaderJsonObj(DataXTaskType taskType) {
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
            parameter.fluentPut("column", buildColumnJsonObjList(Param.columns, true))
                .fluentPut("connection", buildDataSourceJsonObjList(Param.source_dataSource, Param.source_tableName));
            switch (taskType) {
                case FullLoad: {
                    if (StringUtils.isBlank(Param.source_where)
                        && Param.createTimeColumn != null
                    ) {
                        switch (Param.createTimeColumn.getDataType()) {
                            case DATETIME: {
                                parameter.fluentPut("where", String.format("%s >= date_format(${start_ds}, '%%Y%%m%%d%%H%%i%%s') "
                                        + "AND %s <= date_format(${end_ds}, '%%Y%%m%%d%%H%%i%%s')"
                                    , Param.createTimeColumn.getSrcName(), Param.createTimeColumn.getSrcName()));
                                break;
                            }
                            case TIMESTAMP: {
                                parameter.fluentPut("where", String.format("%s >= UNIX_TIMESTAMP(date_format(${start_ds}, '%%Y%%m%%d%%H%%i%%s')) "
                                        + "AND %s <= UNIX_TIMESTAMP(date_format(${end_ds}, '%%Y%%m%%d%%H%%i%%s'))"
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
                                        + "AND %s <= date_format(str_to_date(${bdp.system.cyctime}, '%%Y%%m%%d%%H%%i%%s'), '%%Y-%%m-%%d %%H:%%i:%%s')"
                                    , Param.updateTimeColumn.getSrcName(), Param.schedule_pull_schedule_minutes, Param.updateTimeColumn.getSrcName()));
                                break;
                            }
                            case TIMESTAMP: {
                                parameter.fluentPut("where", String.format("%s >= UNIX_TIMESTAMP(date_format(date_add(str_to_date(${bdp.system.cyctime}, '%%Y%%m%%d%%H%%i%%s'), INTERVAL (-1 * %s) MINUTE), '%%Y-%%m-%%d %%H:%%i:%%s')) "
                                        + "AND %s <= UNIX_TIMESTAMP(date_format(str_to_date(${bdp.system.cyctime}, '%%Y%%m%%d%%H%%i%%s'), '%%Y-%%m-%%d %%H:%%i:%%s'))"
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

    private static JSONObject buildOdpsWriterJsonObj() {
        JSONObject result = new JSONObject();
        do {
            result.fluentPut("stepType", "odps")
                .fluentPut("name", "Writer")
                .fluentPut("category", "writer");
            JSONObject parameter = new JSONObject();
            parameter.fluentPut("truncate", "true")
                .fluentPut("compress", "true")
                .fluentPut("emptyAsNull", "false")
                .fluentPut("partition", (Param.etl_partition + ", dw__plan_time=${dw__plan_time}"))
                .fluentPut("datasource", Param.etl_odpsDataSource)
                .fluentPut("table", Param.tableName_etlTableName)
                .fluentPut("column", buildColumnJsonObjList(Param.columns, false));
            result.fluentPut("parameter", parameter);
        } while (false);
        return result;
    }

    private static JSONObject buildDefaultJsonRootObj() {
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

    public static String createDataXJson4FullLoad() {
        String result = null;
        do {
            JSONObject root = buildDefaultJsonRootObj();
            root.fluentPut("HupunComment", "dw__src_id=1 dw__plan_time=000000000000 start_ds=00000000000000 end_ds=99991231235959");

            JSONArray steps = new JSONArray();
            steps.fluentAdd(buildMysqlReaderJsonObj(DataXTaskType.FullLoad))
                .fluentAdd(buildOdpsWriterJsonObj());

            root.fluentPut("steps", steps);

            result = root.toJSONString();

            if (FileUtils.saveResultToFile(Param.fileName_dataX_fullLoad, result) != 0) {
                break;
            }
        } while (false);
        return result;
    }

    public static String createDataXJson4BlockLoad() {
        String result = null;
        do {
            JSONObject root = buildDefaultJsonRootObj();
            root.fluentPut("HupunComment", "dw__src_id=1 dw__plan_time=$[yyyymmddhh24mi]");

            JSONArray steps = new JSONArray();
            steps.fluentAdd(buildMysqlReaderJsonObj(DataXTaskType.BlockLoad))
                .fluentAdd(buildOdpsWriterJsonObj());

            root.fluentPut("steps", steps);

            result = root.toJSONString();

            if (FileUtils.saveResultToFile(Param.fileName_dataX_blockLoad, result) != 0) {
                break;
            }
        } while (false);
        return result;
    }
}
