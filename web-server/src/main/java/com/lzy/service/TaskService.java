package com.lzy.service;

import com.lzy.entity.TaskBean;
import org.springframework.stereotype.Service;

/**
 * @Author lch
 * @Create on 2017/09/03 12:27
 **/
@Service
public interface TaskService {
    int addTask(TaskBean taskBean) throws Exception;
}
