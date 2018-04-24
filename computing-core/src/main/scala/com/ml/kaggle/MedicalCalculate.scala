package com.ml.kaggle

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/4/24.
  */


object MedicalCalculate{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
    val sc=spark.sparkContext.setLogLevel("warn")

    val medicalCalculate=new MedicalCalculate(spark)
    val data1_path="data_part1.txt"
    val data2_path="data_part2.txt"
    val train_path="train.csv"
    val test_path="test.csv"
    val data_df=medicalCalculate.getDataDF(data1_path)
    data_df.show(false)

  }

}
class MedicalCalculate(spark:SparkSession) {
import spark.implicits._
  val base_path="E:\\dataset\\medicalCalculate\\20180408\\"
def getDataDF(path:String)={
  spark.read.option("header","true")
    .option("nullValue","NA")
    .option("inferSchema","true")
.csv(base_path+path)

}



}
