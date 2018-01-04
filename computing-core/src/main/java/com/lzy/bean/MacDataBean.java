package com.lzy.bean;

/**
 * mac数据实体
 * <p>
 * Created by Liu Zi Yang on 2017/6/18 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class MacDataBean {
    private String router;
    private Integer rssi;   //手机连接的wifi的rssi
    private Double range;    //手机与嗅探器的距离
    private String tmc;     //手机连接的wifi的mac
    private String mac;        //手机的mac
    private String tc;       //手机是否连接上wifi
    private String ds;      //手机是否属于休眠状态

    public MacDataBean(){}
    public MacDataBean(String router, Integer rssi, Double range, String tmc, String mac, String tc, String ds){
        this.router = router;
        this.rssi = rssi;
        this.range = range;
        this.tmc = tmc;
        this.mac = mac;
        this.tc = tc;
        this. ds = ds;
    }
    public String getRouter() {
        return router;
    }

    public void setRouter(String router) {
        this.router = router;
    }

    public int getRssi() {
        return rssi;
    }

    public void setRssi(int rssi) {
        this.rssi = rssi;
    }

    public Double getRange() {
        return range;
    }

    public void setRange(Double range) {
        this.range = range;
    }

    public String getTmc() {
        return tmc;
    }

    public void setTmc(String tmc) {
        this.tmc = tmc;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getTc() {
        return tc;
    }

    public void setTc(String tc) {
        this.tc = tc;
    }

    public String getDs() {
        return ds;
    }

    public void setDs(String ds) {
        this.ds = ds;
    }
}
