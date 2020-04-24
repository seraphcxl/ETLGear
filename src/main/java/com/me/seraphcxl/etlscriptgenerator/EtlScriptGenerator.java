package com.me.seraphcxl.etlscriptgenerator;

import com.alibaba.fastjson.JSONObject;

/**
 * @author xiaoliangchen
 */
public interface EtlScriptGenerator {

    /**
     * 创建 ETL 脚本（json and sql）
     * @return 0：成功；
     */
    int generateScript(JSONObject param);
}
