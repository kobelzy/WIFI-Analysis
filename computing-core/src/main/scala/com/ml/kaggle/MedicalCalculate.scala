package com.ml.kaggle

import breeze.plot._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import MedicalCalculate.data
import org.apache.spark.sql.expressions.UserDefinedFunction
/**
  * Created by Administrator on 2018/4/24.
  */

/** *
  * 病人体检样例中，每个病人的一项体检会使用一条来表示，每个病人有多个体检项目
  */
object MedicalCalculate {
  case class data(vid:String,table_id:String,field_results:String)
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
    val reduceData = medicalCalculate.reduceData(data1_df, data2_df)
    reduceData.show(false)
  }

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
  def reduceData(data1_df: DataFrame, data2_df: DataFrame) = {
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
//      .toDF("via","table_id","field_results")
      .as[data]

    /*
    获取数据集中所有的特征标签
     */
    val tableID_list = spark.sparkContext.broadcast(
      allData_df.groupByKey(_.table_id).keys.collectAsList()
    ).value

    val withColumnName=""
    val vid2tables_ds=allData_df.groupByKey(_.vid)
      .mapGroups{case (vid,iter)=>
        (vid,iter.toList)
        }
    val vid2table_ds=vid2tables_ds.map(_._2
      .filter(case_data => {
        //如果当前行数据有目标tableid，那么将其选出来，如果没有的话结果为空
        case_data.table_id.equals(withColumnName)
      }
      ).map(_.field_results).headOption.orNull
    ).toDF("result")
    vid2tables_ds.withColumn(withColumnName,addFeature(vid2table_ds("result")))
  }

//  val unhotUDF = udf((vec: Vector) => vec.toArray.indexOf(1.0).toDouble)
  val addFeature: UserDefinedFunction =udf((tableID:String)=>tableID)
//val tableIDInData_list=iter.toList.map(_.table_id)

  //for(tableID<-tableID_list){
//  if(tableIDInData_list.contains(tableID)){

  def withColumn(allData_df:Dataset[data],withColumnName:String)={
    val withColumnName=""
    val vid2tables_ds=allData_df.groupByKey(_.vid)
      .mapGroups{case (vid,iter)=>
        (vid,iter.toList)
      }
    val vid2table_ds=vid2tables_ds.map(_._2
      .filter(case_data => {
        //如果当前行数据有目标tableid，那么将其选出来，如果没有的话结果为空
        case_data.table_id.equals(withColumnName)
      }
      ).map(_.field_results).headOption.orNull
    ).toDF("result")
    vid2tables_ds.withColumn(withColumnName,addFeature(vid2table_ds("result")))


  }
//  //包含了当钱需要的特征，将其加入到column中
//}
  /*
  1、异常值处理
   */


}
