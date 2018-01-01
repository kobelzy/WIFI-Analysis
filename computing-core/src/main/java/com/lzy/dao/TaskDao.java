package com.lzy.dao;

import com.lzy.bean.TaskBean;

import java.util.List;


/**
 * 任务信息数据库接口
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public interface TaskDao {
    //查询总数
    int getTaskCount();

    // 查询所有任务信息
    List<TaskBean> getTaskInfo();

    //根据条件查询用户信息
    TaskBean getTaskById(Long id);

    void addTask(TaskBean taskBean);
}
