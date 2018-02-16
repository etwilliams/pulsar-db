package com.dakuupa.pulsar.test;

import com.dakuupa.pulsar.Entity;
import com.dakuupa.pulsar.annotations.DbMysqlText;
import com.dakuupa.pulsar.annotations.DbNotNull;
import com.dakuupa.pulsar.annotations.DbSize;
import com.dakuupa.pulsar.annotations.DbUnique;

/**
 *
 * @author etwilliams
 */
public class Demo extends Entity {

    @DbSize(size = 512)
    private String strValue;
    @DbMysqlText
    private String longStrValue;
    private int iValue;
    private double dvalue;
    private boolean bvalue;
    private float fvalue;
    @DbSize(size = 512)
    @DbUnique
    @DbNotNull
    private String uniqueStr;

    public String getStrValue() {
        return strValue;
    }

    public void setStrValue(String strValue) {
        this.strValue = strValue;
    }

    public String getLongStrValue() {
        return longStrValue;
    }

    public void setLongStrValue(String longStrValue) {
        this.longStrValue = longStrValue;
    }

    public int getiValue() {
        return iValue;
    }

    public void setiValue(int iValue) {
        this.iValue = iValue;
    }

    public String getUniqueStr() {
        return uniqueStr;
    }

    public void setUniqueStr(String uniqueStr) {
        this.uniqueStr = uniqueStr;
    }

    public double getDvalue() {
        return dvalue;
    }

    public void setDvalue(double dvalue) {
        this.dvalue = dvalue;
    }

    public boolean isBvalue() {
        return bvalue;
    }

    public void setBvalue(boolean bvalue) {
        this.bvalue = bvalue;
    }

    public float getFvalue() {
        return fvalue;
    }

    public void setFvalue(float fvalue) {
        this.fvalue = fvalue;
    }

}
