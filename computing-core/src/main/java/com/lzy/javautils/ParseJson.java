package com.lzy.javautils;

import com.alibaba.fastjson.JSON;
import com.lzy.bean.MacDataBean;

import java.util.List;
/**
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class ParseJson {
    public static List<MacDataBean> jsonToList(String json){
        return JSON.parseArray(json, MacDataBean.class);
    }

    public static MacDataBean jsonToObject(String json){
        return JSON.parseObject(json, MacDataBean.class);
    }
}
