package com.lzy.service.impl;

import com.lzy.entity.UserVisitTimeBean;
import com.lzy.mapper.UserVisitTimeMapper;
import com.lzy.service.UserVisitTimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Wang Han on 2017/6/20 16:40.
 * E-mail address is wanghan0501@vip.qq.com.
 * Copyright © 2017 Wang Han. SCU. All Rights Reserved.
 */

@Service("userVisitTimeService")
public class UserVisitTimeServiceImpl implements UserVisitTimeService {

    @Autowired
    private UserVisitTimeMapper userVisitTimeMapper;

    public String getFirstVisitTIme(int shopId, String mac) {
        String firstTime = null;
        try {
            firstTime = userVisitTimeMapper.getFirstVisitTIme(shopId, mac);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        
        return firstTime;
    }

    public List<UserVisitTimeBean> getUserVisitTime(int firstLine, int secondLine) {
        return userVisitTimeMapper.getUserVisitTime(firstLine,secondLine);
    }
}
