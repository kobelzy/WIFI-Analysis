package com.lzy.scalautils

import com.lzy.bean.PropertyBean
import com.lzy.common.constants.AnalysisConstants
import com.lzy.dao.impl.UserVisitTimeDaoImpl
import com.lzy.javautils.DateUtil
import org.apache.log4j.Logger

/**
  * 获取数据工具类
  * Created by Liuziyang on 2017/12/31.
  */
class AnalysisUtil{}

object AnalysisUtil {
  private val logger = Logger.getLogger(classOf[AnalysisUtil])
  private val property:PropertyBean=InitUtil.getPropertyFromDatabase()
  //判断用户是否入店
  def isCheckIn(range:Double,rssi:Int):Boolean={
    if(range<property.getVisitRange) true
    else false
  }

  /**
    * 判断用户是否是深度访问
    * @param shopId
    * @param mac
    * @param time
    * @return
    */
  def isDeepVisit(shopId:Int,mac:String,time:Long):Boolean={
    val userVisitTimeDao=new UserVisitTimeDaoImpl
    val queryResult=userVisitTimeDao.getFirstVisitTime(shopId,mac)
    val firstTime=if(queryResult==AnalysisConstants.DEFAULT_FIRST_VISIT_TIME) time else queryResult
    DateUtil.after(firstTime,time,property.getVisitTimeSplit.toLong)
  }

  /**
    * 计算入店率
    * @param checkInFlow
    * @param totalFlow
    * @return
    */
  def getCheckInRate(checkInFlow:Int,totalFlow:Int):Double={
    var checkInRate:Double=0.0
    try{
      if(totalFlow!=0) checkInRate=checkInFlow.toDouble/totalFlow.toDouble
    }catch{
      case e:ArithmeticException=>{
        checkInRate=0.0
      }
    }
    checkInRate
  }

  /**
    * 计算深访率
    * @param deepVisitFlow
    * @param checkInFlow
    * @return
    */
  def getDeepVisitRate(deepVisitFlow:Int,checkInFlow:Int):Double={
    var deepVisitRate:Double=0.0
    try{
      if(checkInFlow!=0) deepVisitRate=deepVisitFlow.toDouble/checkInFlow.toDouble
    }catch{
      case e:ArithmeticException=>{
        deepVisitRate=0.0
      }
    }
      deepVisitRate
  }

  /**
    * 计算浅访问率
    * @param deepVisitFlow
    * @param checkInFlow
    * @return
    */
  def getShallowVisitRate(deepVisitFlow:Int,checkInFlow:Int):Double={
    var shallowVisitRate:Double=0.0
    try{
      if(checkInFlow!=0) shallowVisitRate=1.0-deepVisitFlow.toDouble/checkInFlow.toDouble
    }catch{
      case e:ArithmeticException=>{
        shallowVisitRate=0.0
      }
    }
shallowVisitRate
  }
}
