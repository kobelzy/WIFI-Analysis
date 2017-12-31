package com.lzy.service.impl;

import com.lzy.entity.entityData.Hour;
import com.lzy.entity.entityData.Month;
import com.lzy.entity.entityData.Year;
import com.lzy.mapper.QueryHistoryDataMapper;
import com.lzy.service.QueryHistoryDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by maicius on 2017/6/28.
 */
@Service
public class QueryHistoryDataServiceImpl implements QueryHistoryDataService {
    @Autowired
    QueryHistoryDataMapper queryHistoryData;

    public int addActivityData() throws Exception {
        return queryHistoryData.addActivityData();
    }

    public List<Year> queryActivityYear() throws Exception {
        return queryHistoryData.queryActivityYear();
    }

    public List<Month> queryActivityMonth() throws Exception {
        return queryHistoryData.queryActivityMonth();
    }

    public List<Hour> queryActivityDay(String date) throws Exception {
        return queryHistoryData.queryActivityDay(date);
    }

}
