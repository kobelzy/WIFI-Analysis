package com.lzy.bean;

import java.io.Serializable;
/**
 * 用户访问时间类
 * <p>
 * Created by Liu Zi Yang on 2017/6/18 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class UserVisitTimeBean implements Serializable {
    private static final long serialVersinUID = 351877796426921776L;

    // 商店ID
    private int shopId;
    // Mac地址
    private String mac;
    // 访问时间
    private long visitTime;

    public static long getSerialVersinUID() {
        return serialVersinUID;
    }

    public int getShopId() {
        return shopId;
    }

    public void setShopId(int shopId) {
        this.shopId = shopId;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public long getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(long visitTime) {
        this.visitTime = visitTime;
    }
}
