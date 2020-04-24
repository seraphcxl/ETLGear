package com.me.seraphcxl.column;

/**
 * @author xiaoliangchen
 */
public abstract class AbstractColumn {
    protected String name;
    protected String comment;
    protected boolean pk;

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public boolean isPk() {
        return pk;
    }
}
