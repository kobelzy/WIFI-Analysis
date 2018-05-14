package com.ml.kaggle.JData

import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * Created by Administrator on 2018/5/13.
  */
object JDataTest {


  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("name")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val jDataTest=new JDataTest(spark)
    val basePath="E:\\dataset\\JData_UserShop\\"
    val sku="jdata_sku_basic_info.csv"
    val user_basic="jdata_user_basic_info.csv"
    val user_action="jdata_user_action.csv"
    val user_order="jdata_user_order.csv"
    val user_comment="jdata_user_comment_score.csv"
    val data=jDataTest.getSourceData(basePath+user_comment)
data.printSchema()
    data.show(false)
//    println(data.count())
//    println(data.na.drop().count())
    data.select($"score_level").groupBy($"score_level").count().show(false)
//    val max2min=data.select($"age".as[Int]).collect()
//    println("max:"+max2min.max)
//    println("min:"+max2min.min)
//    data.select($"o_date").groupBy($"o_date").count().show(40,false)
//    data.select($"o_sku_num").groupBy($"o_sku_num").count().show(false)

//查看空值


  }
  }

  class JDataTest(spark:SparkSession){

  def getSourceData(path:String): DataFrame ={
    val data=spark.read.option("header","true")
      .option("nullValue","NA")
      .option("inferSchema","true")
        .csv(path)
    data
  }

}
