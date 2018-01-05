package com.lzy.javautils;

import com.lzy.common.types.TimeTypes;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 日期工具类
 * Created by Liu Zi Yang on 2017/6/24 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */

public class DateUtil {

    // 得到log记录器
    private static final Logger logger = Logger.getLogger(DateUtil.class);

    /**
     * 获取现在时间
     *
     * @return 此刻时间
     */
    public static long getCurrentTime() {
        return new Date().getTime();
    }

    /**
     * @param time
     * @param timeTypes
     * @return
     */
    public static long parseTime(String time, TimeTypes timeTypes) {
        try {
            switch (timeTypes) {
                case ENGLISH_TIME_FORMAT://english
                    SimpleDateFormat englishTimeFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy", Locale.ENGLISH);
                    return englishTimeFormat.parse(time).getTime();
                case TIME_FORMAT:
                    SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    return timeFormat.parse(time).getTime();
                case DATE_FORMAT:
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    return dateFormat.parse(time).getTime();
            }

        } catch (ParseException e) {
            System.err.println("Current TimeTypes is : " + timeTypes.toString());
            logger.error("Current TimeTypes is : " + timeTypes.toString());
        }

        return 0L;
    }

    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }


    /**
     * 判断第二个时间是否在第一个时间之后
     *
     * @param time1
     * @param time2
     * @param intervalMillisecond 间隔时常，单位毫秒
     * @return
     */
    public static boolean after(long time1, long time2, long intervalMillisecond) {
        if (time2 - time1 > intervalMillisecond) {
            return true;
        } else {
            return false;
        }
    }
}

