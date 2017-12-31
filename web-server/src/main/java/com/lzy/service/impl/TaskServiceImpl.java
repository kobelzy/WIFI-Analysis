package com.lzy.service.impl;

import com.lzy.entity.TaskBean;
import com.lzy.mapper.TaskMapper;
import com.lzy.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Author lch
 * @Create on 2017/09/03 12:27
 **/
@Service
public class TaskServiceImpl implements TaskService {
    @Autowired
    private TaskMapper taskMapper;

    public int addTask(TaskBean taskBean) throws Exception {
        return taskMapper.addTask(taskBean);
    }
}
