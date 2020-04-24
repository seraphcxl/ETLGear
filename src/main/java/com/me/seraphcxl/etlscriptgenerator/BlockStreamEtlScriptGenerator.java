package com.me.seraphcxl.etlscriptgenerator;

import com.alibaba.fastjson.JSONObject;

/**
 * 批次 流水 ETL
 * @author xiaoliangchen
 */
public class BlockStreamEtlScriptGenerator implements EtlScriptGenerator {

    /**
     * 有 full_load.json 和 block_load.json
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
            result = generateDataXBlockLoad();
        } while (false);
        return result;
    }

    protected int generateDataXFullLoad() {
        int result = -1;
        do {
            result = 0;
        } while (false);
        return result;
    }

    protected int generateDataXBlockLoad() {
        int result = -1;
        do {
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
}
