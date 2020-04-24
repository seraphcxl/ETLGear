package com.me.seraphcxl;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

public class MergeOrderBy {
    private String column;
    private String orderBy;

    public MergeOrderBy(String column, String orderBy) {
        Assert.assertTrue("column must not empty!!!", StringUtils.isNotEmpty(column));
        Assert.assertTrue("orderBy must not empty!!!", StringUtils.isNotEmpty(orderBy));

        this.column = column;
        this.orderBy = orderBy;
    }

    public String getColumn() {
        return column;
    }

    public String getOrderBy() {
        return orderBy;
    }
}
