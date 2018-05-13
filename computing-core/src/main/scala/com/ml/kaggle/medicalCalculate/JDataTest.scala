package com.ml.kaggle.medicalCalculate

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/5/13.
  */
object JDataTest {


  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("name")
      .master("local[*")
      .getOrCreate()
    val jDataTest=new JDataTest(spark)
    val basePath="E:\\dataset\\JData_UserShop\\"
    val sku="jdata_user_basic_info.csv"
    val user_basic="jdata_user_basic_info"
    val user_action="jdata_user_action"
    val user_order="jdata_user_order"
    val user_comment="jdata_user_comment_score"
    val data=jDataTest.getSourceData(basePath+sku)
data.printSchema()
    data.show(false)

  }

  class JDataTest(spark:SparkSession){

  def getSourceData(path:String): DataFrame ={
    val data=spark.read.option("header","tre")
      .option("nullValue","NA")
      .option("inferSchema","true")
        .csv(path)
    data
  }
  }
}
