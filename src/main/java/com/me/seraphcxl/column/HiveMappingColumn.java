package com.me.seraphcxl.column;

import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.datatype.HiveDataType;

public class HiveMappingColumn extends HiveColumn {
    private String mapping;

    public HiveMappingColumn(
        String name
        , String srcName
        , HiveDataType dataType
        , String comment
        , boolean pk
        , boolean splitPk
        , boolean createTime
        , boolean updateTime
        , boolean partitionKey
        , String mapping
    ) {
        super(name, srcName, dataType, comment, pk, splitPk, createTime, updateTime, partitionKey);
        this.mapping = mapping;
    }

    public HiveMappingColumn(
        String name
        , String srcName
        , String dataType
        , String comment
        , boolean pk
        , boolean splitPk
        , boolean createTime
        , boolean updateTime
        , boolean partitionKey
        , String mapping
    ) {
        super(name, srcName, dataType, comment, pk, splitPk, createTime, updateTime, partitionKey);
        this.mapping = mapping;
    }

    public String getMapping() {
        return mapping;
    }

    @Override
    public JSONObject toJSONObject() {
        JSONObject result = super.toJSONObject();
        do {
            result.fluentPut("mapping", getMapping());
        } while (false);
        return result;
    }
}
