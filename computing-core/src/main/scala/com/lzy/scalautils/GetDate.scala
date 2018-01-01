package com.lzy.scalautils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by Administrator on 2018/1/1.
  */
object GetDate {
  /**
    * 获取当前时间的前一天的日期
    * @return
    */
  def getYesterday:String={
  val dateFormat:SimpleDateFormat=new SimpleDateFormat("yyyyMMdd")
  val cal:Calendar=Calendar.getInstance()
  cal.add(Calendar.DATE,-1)
  val yesterday=dateFormat.format(cal.getTime)
  yesterday

}
  def getDateTest:String="20170808"


}
