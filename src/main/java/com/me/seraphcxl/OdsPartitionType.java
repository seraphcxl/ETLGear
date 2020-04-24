package com.me.seraphcxl;

/**
 * @author xiaoliangchen
 */

public enum OdsPartitionType {
    Day(0, "自然天; BIGINT")
    , PT(1, "字符串; STRING")
    , OneMonthAndPT(2, "一个月(yyyyMM) + Hash值; BIGINT, BIGINT/STRING")
    ;

    private int code;
    private String desc;

    OdsPartitionType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static OdsPartitionType toOdsPartitionType(int code) {
        OdsPartitionType type = Day;
        switch (code) {
            case 0: {
                type = Day;
                break;
            }
            case 1: {
                type = PT;
                break;
            }
            case 2: {
                type = OneMonthAndPT;
                break;
            }
            default:
                break;
        }
        return type;
    }
}
