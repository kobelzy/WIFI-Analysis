package com.lzy.dao;

import com.lzy.bean.VendorMacBean;

import java.util.List;

/**
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public interface VendorMacDao {
    // 添加信息
    void addVendorMac(VendorMacBean vendorMacBean);

    void addVendorMacBatch(List<VendorMacBean> vendorMacBeanList);

    // 根据Mac前缀获取制造商
    VendorMacBean getVendorByMac(String mac);
}
