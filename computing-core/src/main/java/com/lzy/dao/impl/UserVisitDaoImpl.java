package com.lzy.dao.impl;

import com.alibaba.fastjson.JSON;
import com.lzy.dao.BaseDao;
import com.lzy.bean.UserVisitBean;
import com.lzy.common.constants.TableConstants;
import com.lzy.conf.JedisPoolManager;
import redis.clients.jedis.ShardedJedis;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class UserVisitDaoImpl extends BaseDao {

    @Override
    public void add(List<Object> objectList) {
        ShardedJedis jedis = JedisPoolManager.getResource();
        List<String> values = new ArrayList<>();
        for (Object o : objectList) {
            UserVisitBean userVisitBean = (UserVisitBean) o;
            values.add(JSON.toJSONString(userVisitBean));
        }
        jedis.rpush(TableConstants.TABLE_USER_VISIT, values.toArray(new String[0]));
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
}
