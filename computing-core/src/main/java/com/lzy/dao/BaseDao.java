package com.lzy.dao;

import java.util.List;

/**
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public abstract class BaseDao {

    public abstract void add(List<Object> objectList);

    public abstract Object get(String key);

    public abstract List<Object> get(List<String> keys);
}
