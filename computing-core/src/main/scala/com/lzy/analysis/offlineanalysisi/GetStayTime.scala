package com.lzy.analysis.offlineanalysisi

import com.lzy.dao.impl.UserDaoImpl

/**
  * 离线数据分析
  * Created by Administrator on 2018/1/2.
  */
class GetStayTime extends Runnable{
  override def run(): Unit = {
    while(true){
      val userDao=new UserDaoImpl
      userDao.updateStayTime()
      Thread.sleep(60000)
    }

  }
}
