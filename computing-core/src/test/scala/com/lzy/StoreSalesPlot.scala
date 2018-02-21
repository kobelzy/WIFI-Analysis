package com.lzy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
  * Created by Administrator on 2018/2/19.
  */
object StoreSalesPlot {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("storeSales")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val goal = "Sales"
    val myid = "Id"
    val plot = true
    val path = "F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture07_销量预估\\data"
    val read = spark.read.option("header", "true").option("nullValue", "NA").option("inferSchema", "true")

    val train = read.csv(path + "\\train.csv")
      .withColumn("StateHoliday", $"StateHoliday".cast(StringType))
      .withColumn("DayOfWeek", $"DayOfWeek".cast(StringType))
      .withColumn("Open", $"Open".cast(StringType))
      .withColumn("Promo", $"Promo".cast(StringType))
      .withColumn("SchoolHoliday", $"SchoolHoliday".cast(StringType))




  }
}
