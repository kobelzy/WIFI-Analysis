package com.lzy.conf;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置加载管理类
 * Created by Liu Zi Yang on 2017/6/18 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public class ConfigurationManager {
    // 配置属性
    private static Properties properties = new Properties();
    private static final Logger logger = Logger.getLogger(ConfigurationManager.class);

    static {
        InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            logger.error(e.getStackTrace());
            e.printStackTrace();
        }
    }

    /**
     * 获取关键字对应的配置项
     *
     * @param key
     * @return
     */
    private static synchronized String getProperty(String key) {
        try {
            return properties.getProperty(key);
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        return null;
    }


    /**
     * 获取String配置项
     *
     * @param key
     * @return
     */
    public static synchronized String getString(String key) {
        return getProperty(key);
    }

    /**
     * 获取Integer型配置项
     *
     * @param key
     * @return
     */
    public static synchronized Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            logger.error(e.getStackTrace());
            e.printStackTrace();
        }

        return 0;
    }

    /**
     * 获取Boolean型配置项
     *
     * @param key
     * @return
     */
    public static synchronized Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            logger.error(e.getStackTrace());
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 获取Long型配置项
     *
     * @param key
     * @return
     */
    public static synchronized Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            logger.error(e.getStackTrace());
            e.printStackTrace();
        }

        return 0L;
    }

    /**
     * 获取Double型配置项
     *
     * @param key
     * @return
     */
    public static synchronized Double getDouble(String key) {
        String value = getProperty(key);
        try {
            return Double.valueOf(value);
        } catch (Exception e) {
            logger.error(e.getStackTrace());
            e.printStackTrace();
        }

        return 0.0D;
    }
}
