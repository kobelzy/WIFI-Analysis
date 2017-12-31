package com.lzy.mapper;

import com.lzy.entity.PropertyBean;

/**
 * Created by maicius on 2017/6/28.
 */
public interface PropertyMapper {
    int setProperty(PropertyBean propertyBean) throws Exception;
    int addProperty(PropertyBean propertyBean) throws Exception;
    PropertyBean queryProperty(PropertyBean propertyBean) throws Exception;
}
