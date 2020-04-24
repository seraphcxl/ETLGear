package com.me.seraphcxl.column;

import com.me.seraphcxl.datatype.MySqlDataType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

/**
 * @author xiaoliangchen
 */
public class MySqlColumn extends AbstractColumn {
    private MySqlDataType dataType;

    public MySqlColumn(String name, String comment, MySqlDataType dataType) {
        Assert.assertTrue("name must not empty!!!", StringUtils.isNotEmpty(name));
        this.name = name;
        this.comment = comment;
        this.dataType = dataType;
    }

    public MySqlColumn(String name, String comment, String dataTypeName) {
        this(name, comment, MySqlDataType.toType(dataTypeName));
    }
}
