package com.me.seraphcxl.etlscriptgenerator;

import com.alibaba.fastjson.JSONObject;

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
