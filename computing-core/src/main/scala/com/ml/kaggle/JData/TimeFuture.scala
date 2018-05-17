package com.ml.kaggle.JData

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable
/**
  * Created by Administrator on 2018/5/14.
  */
object TimeFuture {
  val basePath = "E:\\dataset\\JData_UserShop\\"
  val sku = "jdata_sku_basic_info.csv"
  val user_basic = "jdata_user_basic_info.csv"
  val user_action = "jdata_user_action.csv"
  val user_order = "jdata_user_order.csv"
  val user_comment = "jdata_user_comment_score.csv"

  case class Sku_Case(sku_id: Int, price: Double, cate: Integer, para_1: Double, para_2: Int, para_3: Int)

  case class User_Case(user_id: Int, age: Int, sex: Int, user_lv_cd: Int)

  case class Order_Case(user_id: Int, sku_id: Int, o_id: Int, o_date: Timestamp, o_area: Int, o_sku_num: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val timeFuture = new TimeFuture(spark)

    //商品信息,sku_id,price,cate,para_1,para_2,para_3
    val skuSourcd_df = timeFuture.getSourceData(basePath + sku)

    //用户信息,user_id,age,sex,user_lv_cd
    val user_df = timeFuture.getSourceData(basePath + user_basic).cache()
    //用户行为，user_id,sku_id,a_date,a_num,a_type
    //    val action_df=timeFuture.getSourceData(basePath+user_action)
    //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
    val order_df = timeFuture.getSourceData(basePath + user_order).cache()
    //评价表,user_id,comment_create_tm,o_id,score_level
    //    val comment_df=timeFuture.getSourceData(basePath+user_comment)

    order_df.show()
    user_df.show()
    order_df.printSchema()
    /**
      * 做关联,基于订单表
      */
    //        val joins:DataFrame = order_df.join(user_df, "user_id")
    //        joins.printSchema()
    println(user_df.count())
    println(order_df.select("user_id").distinct().count())
  }
}

class TimeFuture(spark: SparkSession) {

  /**
    * 获取csv转换为DF
    *
    * @param path
    * @return
    */
  def getSourceData(path: String): DataFrame = {
    val data = spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .csv(path)
    data
  }

  def unionOrder2Action(order_df: DataFrame, action_df: DataFrame) = {
    val order_columns=Array("user_id","sku_id","o_id","o_date","o_area","o_sku_num")
    //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
//    order_df.groupByKey(_.getAs[Int](0))
//      .flatMapGroups{case Row(user_id:Int,sku_id:Int,o_id:Int,o_area:Int,o_sku_num:Int)=>
//        (sku_id,o_id,o_area,o_sku_num)
//      }
    order_df.map{case Row(user_id:Int,sku_id:Int,o_id:Int,o_date:Timestamp,o_area:Int,o_sku_num:Int)=>(user_id,(sku_id,o_id,o_date,o_area,o_sku_num))}
      .rdd
      .groupByKey()
      .flatMap{case (user_id,iter)=>
       val sku2other_list:List[(Int, (Int, Timestamp, Int, Int))]= iter.toList.map(tuple=>{
         val  (sku_id,o_id,o_date,o_area,o_sku_num)=tuple
          (sku_id,(o_id,o_date,o_area,o_sku_num))
        })
        //聚合到商品粒度
       val sku2other_grouped_list: Map[Int, Unit] = sku2other_list.groupBy(_._1)
            .mapValues { sku2other_list =>
              //根据时间戳进行排序
              val sku2other_sorted_list = sku2other_list.sortBy(_._2._2)
              val list=
              for (i <- sku2other_sorted_list.indices)  {
                val (sku_id, (o_id, o_date, o_area, o_sku_num)) = sku2other_sorted_list(i)
               val o_id2timeZone= i match {
                  //如果为0，那么是最小的时间戳，使用( ,t]格式
                  case 0 => (o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list.head._2._2))
                  //如果是最后一个值，那么使用(t, )
                  case sku2other_sorted_list.length - 1 => (o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list.last._2._2))
                  //那么使用(t-1,t]
                  case _ => (o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list(i - 1)._2._2, o_date))
                }
                o_id2timeZone
              }
            }
        //扩充到用户粒度

          Array("")
      }

  }



























}
