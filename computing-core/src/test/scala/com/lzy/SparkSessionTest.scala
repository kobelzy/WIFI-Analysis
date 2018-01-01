package com.lzy

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/12/31.
  */
object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("test").getOrCreate()
    spark.sparkContext


  }
}
