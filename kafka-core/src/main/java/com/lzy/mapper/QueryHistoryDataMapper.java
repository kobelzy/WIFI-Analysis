package com.lzy.mapper;

import com.lzy.entity.entityData.Hour;
import com.lzy.entity.entityData.Month;
import com.lzy.entity.entityData.Year;

import java.util.List;

/**
 * Created by maicius on 2017/6/29.
 */
public interface QueryHistoryDataMapper {

    int addActivityData() throws Exception;
    List<Year> queryActivityYear() throws Exception;
    List<Month> queryActivityMonth() throws Exception;
    List<Hour> queryActivityDay(String date) throws Exception;
}
