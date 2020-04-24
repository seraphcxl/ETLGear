package com.me.seraphcxl.etlscriptgenerator;

import com.me.seraphcxl.EtlJobType;

/**
 * @author xiaoliangchen
 */
public class EtlScriptGeneratorFactory {
    public static EtlScriptGenerator createETLScriptGenerator(EtlJobType jobType) {
        EtlScriptGenerator result = null;
        do {
            if (jobType == null) {
                break;
            }
            if (jobType.getFullOrBlock().equalsIgnoreCase("block")) {
                if (jobType.isNeedMerge()) {
                    result = new BlockMergeEtlScriptGenerator();
                } else {
                    result = new BlockStreamEtlScriptGenerator();
                }
            } else if (jobType.getFullOrBlock().equalsIgnoreCase("full")) {
                result = new FullEtlScriptGenerator();
            }
        } while (false);
        return result;
    }
}
