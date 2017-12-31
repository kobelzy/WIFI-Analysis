package com.lzy.mapper;

import com.lzy.entity.TaskBean;

/**
 * @Author lch
 * @Create on 2017/09/03 12:26
 **/
public interface TaskMapper {

    int addTask(TaskBean taskBean) throws Exception;
}
