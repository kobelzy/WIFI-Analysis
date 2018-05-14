package com.ml.kaggle.JData

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/5/14.
  */
object TimeFuture{
  val basePath="E:\\dataset\\JData_UserShop\\"
  val sku="jdata_sku_basic_info.csv"
  val user_basic="jdata_user_basic_info.csv"
  val user_action="jdata_user_action.csv"
  val user_order="jdata_user_order_test.csv"
  val user_comment="jdata_user_comment_score.csv"

  case class Sku_Case(sku_id:Int,price:Double,cate:Integer,para_1:Double,para_2:Int,para_3:Int)
  case class User_Case(user_id:Int,age:Int,sex:Int,user_lv_cd:Int)
  case class order_df(user_id:Int,sku_id:Int,o_id:Int,o_date:Timestamp,o_area:Int,o_sku_num:Int)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("names")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val timeFuture=new TimeFuture(spark)

    //商品信息,sku_id,price,cate,para_1,para_2,para_3
    val skuSourcd_df=timeFuture.getSourceData(basePath+sku)
//用户信息,user_id,age,sex,user_lv_cd
    val user_df=timeFuture.getSourceData(basePath+user_basic).as[User_Case]
    //用户行为，user_id,sku_id,a_date,a_num,a_type
//    val action_df=timeFuture.getSourceData(basePath+user_action)
    //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
    val order_df=timeFuture.getSourceData(basePath+user_order).as[order_df]
    //评价表,user_id,comment_create_tm,o_id,score_level
//    val comment_df=timeFuture.getSourceData(basePath+user_comment)

    order_df.show()
    user_df.show()
    /**
      * 做关联,基于订单表
      */
      val od_df=order_df.withColumnRenamed("user_id","user_id1")
    import org.apache.spark.sql.functions._
   val joins=od_df.joinWith(user_df,$"user_id1"===$"user_id")
    joins.show()

  }
}
class TimeFuture(spark:SparkSession) {
  import spark.implicits._

  /**
    * 获取csv转换为DF
    * @param path
    * @return
    */
  def getSourceData(path:String): DataFrame ={
    val data=spark.read.option("header","true")
      .option("nullValue","NA")
      .option("inferSchema","true")
      .csv(path)
    data
  }
}
