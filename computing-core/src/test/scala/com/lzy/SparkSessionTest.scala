package com.lzy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/12/31.
  */
object SparkSessionTest {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("test").master("local[8]").getOrCreate()

   val data1= spark.read.option("header", "true").option("nullValue", "NA").option("inferSchema", "true")
      .csv("D:\\WorkSpace\\ScalaWorkSpace\\WIFI-Analysis\\computing-core\\src\\test\\resources\\test.csv")
    val data=data1.drop("id")
    val data2=data1.describe("id","age","name")
//      .filter(_.getAs[String]("summary")=="mean")
data.show(false)
data.printSchema()
    data2.show(false)
    data2.printSchema()
  }
}
