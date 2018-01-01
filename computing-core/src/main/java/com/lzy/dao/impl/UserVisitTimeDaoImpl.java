package com.lzy.dao.impl;

import com.lzy.bean.UserVisitTimeBean;
import com.lzy.common.constants.AnalysisConstants;
import com.lzy.conf.JedisPoolManager;
import com.lzy.dao.BaseDao;
import redis.clients.jedis.ShardedJedis;

import java.util.List;

/**
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class UserVisitTimeDaoImpl extends BaseDao {

    @Override
    public void add(List<Object> objectList) {
        String key = null;
        ShardedJedis jedis = JedisPoolManager.getResource();

        for (Object o : objectList) {
            UserVisitTimeBean userVisitTimeBean = (UserVisitTimeBean) o;
            key = String.valueOf(userVisitTimeBean.getShopId()) + "||"
                + userVisitTimeBean.getMac();
            jedis.rpush(key, String.valueOf(userVisitTimeBean.getVisitTime()));
        }

        jedis.close();
    }

    @Override
    public Object get(String key) {
        return null;
    }

    @Override
    public List<Object> get(List<String> keys) {
        return null;
    }

    public long getFirstVisitTime(int shopId, String mac) {
        ShardedJedis jedis = JedisPoolManager.getResource();
        String key = shopId + "||" + mac;
        String firstVisitTime = jedis.lindex(key, 0);
        jedis.close();
        if (firstVisitTime == null) {
            return AnalysisConstants.DEFAULT_FIRST_VISIT_TIME;
        } else {
            return Long.valueOf(firstVisitTime);
        }
    }
}
