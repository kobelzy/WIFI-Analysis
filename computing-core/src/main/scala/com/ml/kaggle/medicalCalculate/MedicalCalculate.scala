package com.ml.kaggle.medicalCalculate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import MedicalCalculate.{data,id2dataInList,addFeature}
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

    val data1_test = medicalCalculate.getDataDF("data_mini.csv", "$")
        val data2_test = medicalCalculate.getDataDF("data_mini.csv", "$")
        val reduceData = medicalCalculate.reduceData(data1_df, data2_df)
    reduceData.coalesce(1).saveAsTextFile("E:\\dataset\\medicalCalculate\\20180408\\all_data")
//    medicalCalculate.getItemInfomation(data1_df,data2_df)
//    medicalCalculate.showInterminateDatas(data1_df,data2_df)
  }

  val addFeature: UserDefinedFunction = udf((list: Seq[data]) =>
    list.filter(_.table_id.equals("0102")).map(_.field_results).headOption.orNull[String]
  )


}

class MedicalCalculate(spark: SparkSession) {
  import spark.implicits._

  def showInterminateDatas(data1_df: DataFrame, data2_df: DataFrame): Unit ={
    val intlerminateArr=Array("319276","21A163","269066","G99110","549016","269062","300096","269067","459269","949006","21A097","21A165","300095","339113","179216","509061","4699","300103","549012","G49058","300090","E49012","21A010","21A006","949005","B19012","269063","300100","4967","Q49011","459270","539010","319177","E49009","B19010","I19028","549013","669013","2265","21A059","300094","I59002","21A108","269064","2264","B19009","509062","459326","509063","269065","809071","21A008")
    val allData_df = data1_df.union(data2_df)
      .na.fill("")
      .as[data]
    allData_df.filter(data=>intlerminateArr.contains(data.table_id)).groupByKey(_.table_id).mapGroups{case (table_id,iter)=>
      val site=iter.map(_.field_results).toList.distinct
        val siteSize=site.mkString("|")
      (table_id,site.size,siteSize)
    }.coalesce(1).write.csv("E:\\dataset\\medicalCalculate\\classicNum\\item2fieldResult.csv")


  }


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


  val base_path = "E:\\dataset\\medicalCalculate\\20180408\\"

  def getDataDF(path: String, sep: String): DataFrame = {
    spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("sep", sep)
      .csv(base_path + path)

  }
def getItemInfomation(data1_df: DataFrame, data2_df: DataFrame)={

  val allData_df = data1_df.union(data2_df)
    .na.fill("")
    .as[data]


    allData_df.groupByKey(_.table_id).mapGroups{case (table_id,iter)=>
    val site=iter.map(_.field_results).toList.distinct.size
      (table_id,site)
    }.coalesce(1).write.csv("E:\\dataset\\medicalCalculate\\classicNum\\item2Num_distinct.csv")

}
  /**
    * 特征工程
    */
  /*
  0、实际场景预处理
   */
  def reduceData(data1_df: DataFrame, data2_df: DataFrame):RDD[String] = {

    val allData_df = data1_df.union(data2_df)
        .na.fill("")
      .as[data]
    /*
    获取数据集中所有的特征标签
     */
    val tableID_arr = spark.sparkContext.broadcast(
      allData_df.groupByKey(_.table_id).keys.collect()
    ).value
    println(tableID_arr.mkString("$"))
    val vid2tables_rdd: RDD[(String, List[data])] = allData_df.groupByKey(_.vid)
      .mapGroups { case (vid, iter) =>
        (vid, iter.toList)
      }.rdd

   val fieldResults_rdd= vid2tables_rdd.map { case (vid, data_list) =>
      val tableIDInData_list = data_list.map(_.table_id)
      val fieldResult_List = mutable.ListBuffer[String]()
      fieldResult_List+=vid
      for (tableID <- tableID_arr) {
        val index = tableIDInData_list.indexOf(tableID)
        val fieldResult: String = index match {
          case -1 => ""
          case n => data_list(n).field_results
        }
        if(fieldResult.contains("<")||fieldResult.contains(">")){
          fieldResult_List += ""
        }else if(fieldResult.equals("-")) {
          fieldResult_List += "0.0"
        }else {
          fieldResult_List += fieldResult
        }
      }
      fieldResult_List.toList.mkString("$")
    }
    fieldResults_rdd
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
