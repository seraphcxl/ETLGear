package com.me.seraphcxl.column;

import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.datatype.HiveDataType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

public class HiveColumn extends AbstractColumn {
    public static final HiveColumn dw__src_id = new HiveColumn("dw__src_id", HiveDataType.INT,
        "数据源分组ID");
    public static final HiveColumn dw__plan_time = new HiveColumn("dw__plan_time", HiveDataType.STRING,
        "Task计划运行时间 年月日时分 202001010015");
    public static final HiveColumn ds = new HiveColumn("ds", HiveDataType.BIGINT,
        "年月日 20200101");
    public static final HiveColumn dm = new HiveColumn("dm", HiveDataType.BIGINT,
        "年月 202001");

    private String srcName;
    private HiveDataType dataType;
    private boolean splitPk = false;
    private boolean createTime = false;
    private boolean updateTime = false;
    private boolean partitionKey = false;

    public HiveColumn(
        String name
        , String srcName
        , HiveDataType dataType
        , String comment
        , boolean pk
        , boolean splitPk
        , boolean createTime
        , boolean updateTime
        , boolean partitionKey
    ) {
        Assert.assertTrue("name must not empty!!!", StringUtils.isNotEmpty(name));

        this.name = name;
        this.dataType = dataType;
        this.srcName = srcName;
        this.comment = comment;
        this.pk = pk;
        this.splitPk = splitPk;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.partitionKey = partitionKey;
    }

    /**
     * JSON 构造 object 用的构造函数
     * @param name
     * @param srcName
     * @param dataType
     * @param comment
     * @param pk
     * @param splitPk
     * @param createTime
     * @param updateTime
     * @param partitionKey
     */
    public HiveColumn(
        String name
        , String srcName
        , String dataType
        , String comment
        , boolean pk
        , boolean splitPk
        , boolean createTime
        , boolean updateTime
        , boolean partitionKey
    ) {
        this(name, srcName, HiveDataType.toType(dataType), comment
            , pk, splitPk, createTime, updateTime, partitionKey);
    }

    public HiveColumn(String name, HiveDataType dataType, String comment) {
        this(name, "", dataType, comment
            , false, false, false, false, false);
    }

    public HiveColumn(String name, String srcName, HiveDataType dataType, String comment) {
        this(name, srcName, dataType, comment
            , false, false, false, false, false);
    }

    public HiveDataType getDataType() {
        return dataType;
    }

    public String getDataTypeString() {
        return dataType.name();
    }

    public String getSrcName() {
        if (StringUtils.isEmpty(srcName)) {
            return name;
        } else {
            return srcName;
        }
    }

    public boolean isSplitPk() {
        return splitPk;
    }

    public boolean isCreateTime() {
        return createTime;
    }

    public boolean isUpdateTime() {
        return updateTime;
    }

    public boolean isPartitionKey() {
        return partitionKey;
    }

    public JSONObject toJSONObject() {
        JSONObject result = new JSONObject();
        do {
            result.fluentPut("name", getName())
                .fluentPut("srcName", getSrcName())
                .fluentPut("dataType", getDataTypeString())
                .fluentPut("comment", getComment())
                .fluentPut("pk", isPk())
                .fluentPut("splitPk", isSplitPk())
                .fluentPut("createTime", isCreateTime())
                .fluentPut("updateTime", isUpdateTime())
                .fluentPut("partitionKey", isPartitionKey())
            ;
        } while (false);
        return result;
    }
}
