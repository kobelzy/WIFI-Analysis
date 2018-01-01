package com.lzy.dao;

import com.lzy.bean.PropertyBean;

/**
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public interface PropertyDao {

    // 通过ID从数据库中获取配置
    PropertyBean getPropertyById(int id);

    // 从数据库中拉去最新配置数据，即最后一条数据
    PropertyBean getNewProperty();

    // 使一条配置失效
    void setyPropertyNotUse(PropertyBean propertyBean);

    void setPropertyUse(PropertyBean propertyBean);

    // 判断一条配置是否有效，通过商店ID，以及探针Mac地址
    boolean isUse(int shopId, String mmac);
}
