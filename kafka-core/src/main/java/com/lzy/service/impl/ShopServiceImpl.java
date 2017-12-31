package com.lzy.service.impl;

import com.lzy.entity.ProbeInfo;
import com.lzy.service.ShopService;
import com.lzy.entity.ShopInfo;
import com.lzy.mapper.ShopMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by maicius on 2017/6/27.
 */
@Service
public class ShopServiceImpl implements ShopService {

    @Autowired
    ShopMapper shopMapper;


    public List<ShopInfo> queryShopInfos(ShopInfo shopInfo) throws Exception {
        return shopMapper.queryShopInfos(shopInfo);
    }

    public int addShopInfo(ShopInfo shopInfo) throws Exception{
        return shopMapper.addShopInfo(shopInfo);
    }

    public int updateShopInfo(ShopInfo shopInfo) throws Exception{
        return shopMapper.updateShopInfo(shopInfo);
    }

    public long getUniqueShopId() throws Exception {
        return shopMapper.getUniqueShopId();
    }

    public List<ProbeInfo> queryProbeInfos(ProbeInfo probeInfo) throws Exception{
        return shopMapper.queryProbeInfos(probeInfo);
    }

    public List<ProbeInfo> queryshopProbeInfos(ShopInfo shopInfo) throws Exception {
        return shopMapper.queryShopProbeInfo(shopInfo);
    }

    public int addProbeInfo(ProbeInfo probeInfo) {
        return shopMapper.addProbeInfo(probeInfo);
    }

    public List<ShopInfo> queryShopNameById(ShopInfo shopInfo) {
        return shopMapper.queryShopNameById(shopInfo);
    }
}
