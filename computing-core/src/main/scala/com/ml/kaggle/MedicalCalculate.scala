package com.ml.kaggle

import org.apache.spark.sql.{DataFrame, SparkSession}

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
    val data1_df=medicalCalculate.getDataDF(data2_path,"$")
    val data2_df=medicalCalculate.getDataDF(data2_path,"$")

    val data_df=data1_df.join(data2_df,Seq("vid"),)
    println(data_df.count())
    println(data1_df.count())
    print(data2_df.count())
//    data1_df.show(false)
data_df.printSchema()
  }

}
class MedicalCalculate(spark:SparkSession) {
import spark.implicits._
  val base_path="E:\\dataset\\medicalCalculate\\20180408\\"
def getDataDF(path:String,sep:String): DataFrame ={
  spark.read.option("header","true")
    .option("nullValue","NA")
    .option("inferSchema","true")
      .option("sep",sep)
.csv(base_path+path)

}



}
