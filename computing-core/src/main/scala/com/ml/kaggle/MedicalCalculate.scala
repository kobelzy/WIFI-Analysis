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
    val data1_path="data_part1.txt"//3,673,450   ggg根据via去重之后
    val data2_path="data_part2.txt"//3,673,450   根据via去重之后
    val train_path="train.csv"
    val test_path="test.csv"
    val data1_df=medicalCalculate.getDataDF(data2_path,"$")
    val data2_df=medicalCalculate.getDataDF(data2_path,"$")
    import spark.implicits._
//    val data_df=data1_df.join(data2_df,"vid")
    val data_df1=data1_df.groupByKey(_.getAs[String](0)).count()
    val data_df2=data1_df.groupByKey(_.getAs[String](0)).count()

    println(data_df1.count())
    println(data_df2.count())
    println(data1_df.count())
    print(data2_df.count())
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
