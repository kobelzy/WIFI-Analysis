package com

import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by Administrator on 2018/5/6.
  */
object smax {
  def main(args: Array[String]): Unit = {
    val result=Double.NaN
    println(result.toString)
    println(result.toDouble)
    println(Try(result.toString.toDouble).isFailure && result.toString.length > 4)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()

    val feauture=Array("c1","c2","c3")
    val df=spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv("E:\\dataset\\medicalCalculate\\test.txt").toDF(feauture:_*)
    val select =new VectorAssembler()
      .setInputCols(feauture)
      .setOutputCol("features")
    val df2=select.transform(df)
    df2.printSchema()
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(df2)
    scalerModel.transform(df2).show(false)

  }
}
