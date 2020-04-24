package com.me.seraphcxl.etlscriptgenerator;

import com.alibaba.fastjson.JSONObject;
import com.me.seraphcxl.DataXJsonBuilder;

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
}
