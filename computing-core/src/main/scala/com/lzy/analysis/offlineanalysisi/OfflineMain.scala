package com.lzy.analysis.offlineanalysisi

import com.lzy.common.constants.SparkConstants
import com.lzy.scalautils.{GetDate, InitUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{CompareFilter, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}

/**
  * Created by Administrator on 2018/1/3.
  */
object OfflineMain extends Runnable{
  override def run(): Unit = {
    while(true){
      OfflineMainFunc()
      Thread.sleep(6000*60)//睡
    }
  }
  def OfflineMainFunc():Unit={
    println("enter offline")
    val spark=InitUtil.initSparkSession()
    val sc=spark.sparkContext
    val scan:Scan=new Scan()
    val conf=HBaseConfiguration.create()
    conf.set(SparkConstants.SPARK_ZOOKEEPER,SparkConstants.SPARK_ZOOKEEPER_PORT)    //hbase——zookeeper端口
    conf.set(SparkConstants.SPARK_ZOOKEEPER_QUORUM,SparkConstants.SPARK_ZOOKEEPER_QUORUM_IP)  //hbase-zookeeper服务器
    conf.addResource(SparkConstants.SPARK_HBASE_CONF)   //hbase-site.xml位置
    conf.set(TableInputFormat.INPUT_TABLE,SparkConstants.GROUP_DATA_TABLE)  //设置表名
    val yesterday=GetDate.getYesterday//获取当前时间的前一天
    val rowRegexp=yesterday+"[0-9]{10}\\-+[0-9]{3}"//
    val filter=new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(rowRegexp))//使用正则表达式进行行过滤，配置过滤器
    scan.setFilter(filter)//为扫描器配置过滤器
    val scan_str:String=TableMapReduceUtil.convertScanToString(scan)//将扫描结果转换为String类型
    conf.set(TableInputFormat.SCAN,scan_str)
    AnalysisEachDao.analysisStayTime(sc,conf)
  }
}
