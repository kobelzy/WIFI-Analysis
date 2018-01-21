package com.lzy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/1/14.
  */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    println("kaishi ")
    val spark=SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val sc=spark.sparkContext
    val accumu=sc.longAccumulator("longSum")
    val rdd:RDD[Int]=sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    val result=rdd.map(x=>{
      if(x%2==0) accumu.add(1L)
      1
    })
    result.count()
    println("结果："+accumu.value)
    val result2=rdd.map(x=>{
      if(x%2==0) accumu.add(1L)
      1
    })

    result2.cache().count()
    result2.cache
    result2.count()
    println("结果："+accumu.value)
  }
}
