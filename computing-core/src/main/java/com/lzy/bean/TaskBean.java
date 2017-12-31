package com.lzy.bean;

import java.io.Serializable;

/**
 * 任务实体
 * <p>
 * Created by Liu Zi Yang on 2017/6/18 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 * */
public class TaskBean implements Serializable {

    private static final long serialVersinUID = 351877796426921776L;

    private long taskId;
    private String taskName;
    private long createTime;
    private long startTime;
    private long finishTime;
    private String taskType;
    private String taskStatus;
    private String taskParam;

    public static long getSerialVersinUID() {
        return serialVersinUID;
    }

    public TaskBean() {
    }

    public TaskBean(long taskId, String taskName, long createTime, long startTime, long finishTime, String taskType, String taskStatus, String taskParam) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.createTime = createTime;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.taskType = taskType;
        this.taskStatus = taskStatus;
        this.taskParam = taskParam;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }

    public String getTaskParam() {
        return taskParam;
    }

    public void setTaskParam(String taskParam) {
        this.taskParam = taskParam;
    }
}