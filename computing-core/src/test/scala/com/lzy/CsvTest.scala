package com.lzy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, SparkSession}

/**
  * Created by Administrator on 2018/1/28.
  */
object CsvTest {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hourse_price")
      .master("local[*]")
      .getOrCreate()
    /**
      * 数据准备
      */
    val test = spark.read
      .option("header", "true")
      //      .option("nullValue", "?")
      .option("nanValue", "NA")
      .option("inferSchema", "true")
//      .option("nullValue", "NaN")
      .csv("F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture02_房价预测\\house price\\house_price_input\\test.csv")
    //test.show(10,false)
//        test.printSchema()
    //    test.schema.foreach(field=>{
    //      field.dataType.typeName match {
    //      case "integer" =>println("integer")
    //      case "double" => println("double")
    //      case "long"=>println("long")
    //          _=>println("其他")
    //    }
    //    })
    implicit val matchError = org.apache.spark.sql.Encoders.scalaInt
//    implicit val doubels =    org.apache.spark.sql.Encoders.scalaDouble
    val result= test.select("BsmtHalfBath")
//      .map(row => row.getAs[Int](0))
result.foreach(println(_))
    println(test.count())
    println(result.count())
  }
}
