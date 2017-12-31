package com.lzy.service;

/**
 * Created by maicius on 2017/6/27.
 */

import com.lzy.entity.ProbeInfo;
import com.lzy.entity.ShopInfo;

import java.util.List;

public interface ShopService {

    List<ShopInfo> queryShopInfos(ShopInfo shopInfo) throws Exception;
    int addShopInfo(ShopInfo shopInfo) throws Exception;
    int updateShopInfo(ShopInfo shopInfo) throws Exception;
    long getUniqueShopId() throws  Exception;
    List<ProbeInfo> queryProbeInfos(ProbeInfo probeInfo) throws Exception;
    List<ProbeInfo> queryshopProbeInfos(ShopInfo shopInfo) throws Exception;
    int addProbeInfo(ProbeInfo probeInfo);
    List<ShopInfo> queryShopNameById(ShopInfo shopInfo);
}
