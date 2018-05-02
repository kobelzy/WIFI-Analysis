package com.ml.kaggle

import com.ml.kaggle.MedicalCalculate.{addFeature, data, id2dataInList}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.collection.mutable
/**
  * Created by Administrator on 2018/4/24.
  */

/** *
  * 病人体检样例中，每个病人的一项体检会使用一条来表示，每个病人有多个体检项目
  */
object MedicalCalculate {

  case class data(vid: String, table_id: String, field_results: String)

  case class id2dataInList(vid: String, list: Seq[data])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
    val sc = spark.sparkContext.setLogLevel("warn")

    val medicalCalculate = new MedicalCalculate(spark)
    val data1_path = "data_part1.txt"
    //3,673,450   ggg根据via去重之后 57298
    val data2_path = "data_part2.txt"
    //3,673,450   根据via去重之后 57298
    val train_path = "train.csv"
    val test_path = "test.csv"
    val data1_df = medicalCalculate.getDataDF(data1_path, "$")
    val data2_df = medicalCalculate.getDataDF(data2_path, "$")
    val train_df = medicalCalculate.getDataDF(train_path, ",")
    val test_df = medicalCalculate.getDataDF(test_path, ",")
import spark.implicits._
    data1_df.union(data2_df)        .na.fill("")
      .as[data].groupByKey(_.table_id).mapGroups{case (table_id,iter)=>
      val fields:List[String]=iter.map(_.field_results).toList.distinct
      (table_id,fields.size)
    }.coalesce(1).write.csv("E:\\dataset\\medicalCalculate\\classicNum\\itme2NumByDistinct.csv")

//    val data1_test = medicalCalculate.getDataDF("data_mini.csv", "$")
    //    val data2_test = medicalCalculate.getDataDF("data_mini.csv", "$")
    //    val reduceData = medicalCalculate.reduceData(data1_test, data2_test)
    //    reduceData.foreach(println)
  }

  val addFeature: UserDefinedFunction = udf((list: Seq[data]) =>
    list.filter(_.table_id.equals("0102")).map(_.field_results).headOption.orNull[String]
  )


}

class MedicalCalculate(spark: SparkSession) {
  /**
    * 特征工程部分
    * 1、异常值处理
    * 2、缺失值处理
    * 3、数据标准化
    * 4、特征编码转换
    * 5、特征选择
    *
    *
    */

  import spark.implicits._

  val base_path = "E:\\dataset\\medicalCalculate\\20180408\\"

  def getDataDF(path: String, sep: String): DataFrame = {
    spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("sep", sep)
      .csv(base_path + path)

  }

  /**
    * 特征工程
    */
  /*
  0、实际场景预处理
   */
  def reduceData(data1_df: DataFrame, data2_df: DataFrame):RDD[(String, List[String])] = {
    //    val weightedClusterEntropy = clusterLabel.
    //      // Extract collections of labels, per cluster
    //      groupByKey { case (cluster, _) => cluster }.
    //      mapGroups { case (_, clusterLabels) =>
    //        val labels = clusterLabels.map { case (_, label) => label }.toSeq
    //        // Count labels in collections
    //        val labelCounts = labels.groupBy(identity).values.map(_.size)
    //        labels.size * entropy(labelCounts)
    //      }.collect()
    //首先，将每个用户变为一行，每一个特征作为一列
    //    data1_df.printSchema()
    //    data1_df.groupByKey(_.getString(0))
    //            .mapGroups{case (via,iter)=>
    //              (via,iter.toList.head.getString(1))
    //            }
    //      .toDF("via","features")

    val allData_df = data1_df.union(data2_df)
        .na.fill("")
      .as[data]
    /*
    获取数据集中所有的特征标签
     */
    val tableID_arr = spark.sparkContext.broadcast(
      allData_df.groupByKey(_.table_id).keys.collect()
    ).value

    val vid2tables_rdd: RDD[(String, List[data])] = allData_df.groupByKey(_.vid)
      .mapGroups { case (vid, iter) =>
        (vid, iter.toList)
      }.rdd

    vid2tables_rdd.map { case (vid, data_list) =>
      val tableIDInData_list = data_list.map(_.table_id)
      val fieldResult_List = mutable.ListBuffer[String]()
      for (tableID <- tableID_arr) {
        val index = tableIDInData_list.indexOf(tableID)
        val fieldResult: String = index match {
          case -1 => ""
          case n => data_list(n).field_results
        }
        fieldResult_List += fieldResult
      }
      (vid, fieldResult_List.toList)
    }
  }



    //for(tableID<-tableID_list){
    //  if(tableIDInData_list.contains(tableID)){

    /**
      * 增加一列
      *
      * @param vid2tables_ds
      * @param withColumnName
      * @return
      */
    def withColumn(vid2tables_ds: Dataset[id2dataInList], withColumnName: String): DataFrame = {
      vid2tables_ds.withColumn(withColumnName, addFeature(vid2tables_ds.col("list")))
    }

    //  //包含了当钱需要的特征，将其加入到column中
    //}
    /*
    1、异常值处理
     */


  }
