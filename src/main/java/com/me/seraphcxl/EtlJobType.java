package com.me.seraphcxl;

/**
 * @author xiaoliangchen
 *
 * 支持的 EtlJobType
 *  1. ["full", 0/1, 0/1/2]  用DataX拉数据下来，再做分区
 *  2. ["block", 0, 0/1/2]  只用DataX落数据下来
 *  3. ["block", 1, 0/1/2]  正常的增量ETL
 */
public class EtlJobType {
    String fullOrBlock;
    boolean needMerge;
    OdsPartitionType odsPartitionType;

    public EtlJobType(String fullOrBlock, boolean needMerge, int partitionType) {
        this.fullOrBlock = fullOrBlock;
        this.needMerge = needMerge;
        this.odsPartitionType = OdsPartitionType.toOdsPartitionType(partitionType);
    }

    public String getFullOrBlock() {
        return fullOrBlock;
    }

    public boolean isNeedMerge() {
        return needMerge;
    }

    public OdsPartitionType getOdsPartitionType() {
        return odsPartitionType;
    }

    public boolean valid() {
        boolean result = false;
        do {
            if ("full".equals(fullOrBlock)) {
                if (needMerge) {
                    break;
                }
                result = true;
            } else if ("block".equals(fullOrBlock)) {
                if (needMerge) {
                    if (odsPartitionType == OdsPartitionType.PT) {
                        break;
                    }
                } else {
                    if (odsPartitionType == OdsPartitionType.PT
                        || odsPartitionType == OdsPartitionType.OneMonthAndPT
                    ) {
                        break;
                    }
                }
                result = true;
            }
        } while (false);
        return result;
    }
}
