package com.lzy.bean;

import java.io.Serializable;

/**
 * 店铺探针配置项实体
 * <p>
 * Created by Liu Zi Yang on 2017/6/18 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class PropertyBean implements Serializable {
    private static final long serialVersinUID = 351877796426921776L;

    private int propertyId;    //配置项id
    private int shopId;        //店铺id
    private String mmac;          //嗅探器连接的wifi的mac
    private String visitCycle;    //访问wifi周期
    private double visitRange;    //访问wifi距离
    private int visitRSSI;         //wifirssi （名称）
    private String activityDegree;   
    private String visitTimeSplit;
    private boolean propertyType;  //配置类型

    public PropertyBean() {

    }

    public static long getSerialVersinUID() {
        return serialVersinUID;
    }

    public int getPropertyId() {
        return propertyId;
    }

    public void setPropertyId(int propertyId) {
        this.propertyId = propertyId;
    }

    public int getShopId() {
        return shopId;
    }

    public void setShopId(int shopId) {
        this.shopId = shopId;
    }

    public String getMmac() {
        return mmac;
    }

    public void setMmac(String mmac) {
        this.mmac = mmac;
    }

    public String getVisitCycle() {
        return visitCycle;
    }

    public void setVisitCycle(String visitCycle) {
        this.visitCycle = visitCycle;
    }

    public double getVisitRange() {
        return visitRange;
    }

    public void setVisitRange(double visitRange) {
        this.visitRange = visitRange;
    }

    public int getVisitRSSI() {
        return visitRSSI;
    }

    public void setVisitRSSI(int visitRSSI) {
        this.visitRSSI = visitRSSI;
    }

    public String getActivityDegree() {
        return activityDegree;
    }

    public void setActivityDegree(String activityDegree) {
        this.activityDegree = activityDegree;
    }

    public String getVisitTimeSplit() {
        return visitTimeSplit;
    }

    public void setVisitTimeSplit(String visitTimeSplit) {
        this.visitTimeSplit = visitTimeSplit;
    }

    public boolean getPropertyType() {
        return propertyType;
    }

    public void setPropertyType(boolean propertyType) {
        this.propertyType = propertyType;
    }
}
