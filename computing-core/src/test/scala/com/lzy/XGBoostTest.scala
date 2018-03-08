package com.lzy

import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by Administrator on 2018/3/8.
  */
object XGBoostTest {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf()
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Booster]))
    val spark=SparkSession.builder().master("local[*]").appName("xgboost")
        .config(sparkConf).getOrCreate()
    val df:DataFrame=spark.read.format("libsvm").load("src/main/resources/linear.txt")
    val Array(trainDF,testDF):Array[Dataset[Row]]=df.randomSplit(Array(0.8,0.2))
    trainDF.show()
  }
}
