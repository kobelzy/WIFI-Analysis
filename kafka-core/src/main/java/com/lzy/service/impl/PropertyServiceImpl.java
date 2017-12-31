package com.lzy.service.impl;

import com.lzy.entity.PropertyBean;
import com.lzy.mapper.PropertyMapper;
import com.lzy.service.PropertyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by maicius on 2017/6/28.
 */
@Service
public class PropertyServiceImpl implements PropertyService {

    @Autowired
    private PropertyMapper propertyMapper;
    public int addProperty(PropertyBean propertyBean) throws Exception {
        return propertyMapper.addProperty(propertyBean);
    }

    public int setProperty(PropertyBean propertyBean) throws Exception {
        return propertyMapper.setProperty(propertyBean);
    }

    public PropertyBean queryProperty(PropertyBean propertyBean) throws Exception{
        return propertyMapper.queryProperty(propertyBean);
    }
}
