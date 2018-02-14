package com.ml.kaggle

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/2/14.
  */
object StoreSalesCompetition {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("storeSales")
      .master("local[*]")
      .getOrCreate()

    val goal="Sales"
    val myid="Id"
    val plot=true




  }
}
