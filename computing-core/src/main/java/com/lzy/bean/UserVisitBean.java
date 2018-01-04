package com.lzy.bean;

import java.io.Serializable;

/**
 * 用户访问类
 * <p>
 * Created by Liu Zi Yang on 2017/6/18 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class UserVisitBean implements Serializable {
    private static final long serialVersinUID = 351877796426921776L;

    private int shopId;     //店铺id
    private String mmac;    //嗅探器mac
    private long time;    //时间戳
    private int totalFlow;   //总流量
    private int checkInFlow;    //
    private double checkInRate;    //
    private double shallowVisitRate;    //浅访问频率
    private double deepVisitRate;     //深访问频率

    public UserVisitBean() {

    }

    public static long getSerialVersinUID() {
        return serialVersinUID;
    }

    public long getShopId() {
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

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(int totalFlow) {
        this.totalFlow = totalFlow;
    }

    public long getCheckInFlow() {
        return checkInFlow;
    }

    public void setCheckInFlow(int checkInFlow) {
        this.checkInFlow = checkInFlow;
    }

    public double getCheckInRate() {
        return checkInRate;
    }

    public void setCheckInRate(double checkInRate) {
        this.checkInRate = checkInRate;
    }

    public double getShallowVisitRate() {
        return shallowVisitRate;
    }

    public void setShallowVisitRate(double shallowVisitRate) {
        this.shallowVisitRate = shallowVisitRate;
    }

    public double getDeepVisitRate() {
        return deepVisitRate;
    }

    public void setDeepVisitRate(double deepVisitRate) {
        this.deepVisitRate = deepVisitRate;
    }
}
