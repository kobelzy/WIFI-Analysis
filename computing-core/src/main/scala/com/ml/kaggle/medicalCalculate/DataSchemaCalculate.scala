package com.ml.kaggle.medicalCalculate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import DataSchemaCalculate.{distinct_threshold,sum_threshold}
import scala.collection.mutable
/**
  * Created by Administrator on 2018/4/24.
  */

/** *
  * 病人体检样例中，每个病人的一项体检会使用一条来表示，每个病人有多个体检项目
  */
object DataSchemaCalculate {

  case class data(vid: String, table_id: String, field_results: String)

  case class id2dataInList(vid: String, list: Seq[data])

  val distinct_threshold=5
  val sum_threshold=20
  def main(args: Array[String]): Unit = {
    val base_path = "E:\\dataset\\medicalCalculate\\20180408\\"
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext.setLogLevel("WARN")

    val medicalCalculate = new DataSchemaCalculate(spark)
    val item2Num_path="E:\\dataset\\medicalCalculate\\classicNum\\item2Num.csv"
    val item2Num_distinct_path="E:\\dataset\\medicalCalculate\\classicNum\\item2Num_distinct.csv"
    val item2Num_df=medicalCalculate.getDataDF(item2Num_path,",")
    val item2Num_distinct_df=medicalCalculate.getDataDF(item2Num_distinct_path,",")

    val item2Num2Distinct_df=item2Num_df.join(item2Num_distinct_df,item2Num_df("table_id")===item2Num_distinct_df("table_id"))
    println("item:"+item2Num2Distinct_df.count())
    val all_result_path = "all_result.csv"
    val all_result_df = medicalCalculate.getDataDF(base_path+all_result_path, "$")
//
    val types:Array[(String, String)]=all_result_df.drop("vid").dtypes
    val (strTypes,numericalTypes)=types.partition(_._2.equals("StringType"))
    val item2Num_str_df=item2Num2Distinct_df.filter(row=>strTypes.map(_._1.trim).contains(row.getAs[String](0).trim))
    val item2Num_numerical_df=item2Num2Distinct_df.filter(row=>numericalTypes.map(_._1.trim).contains(row.getAs[String](0).trim))
    println(strTypes.length)
    println(item2Num_str_df.count())
    println(numericalTypes.length)
    println(item2Num_numerical_df.count())
    val fitem2Num_str_df=item2Num2Distinct_df.filter(row=> !strTypes.map(_._1.trim).contains(row.getString(0).trim))
    val fitem2Num_numerical_df=item2Num2Distinct_df.filter(row=> !numericalTypes.map(_._1.trim).contains(row.getString(0).trim))

    println(fitem2Num_str_df.count())
    println(fitem2Num_numerical_df.count())
    strTypes
    item2Num_str_df.except(fitem2Num_numerical_df).show(false)
    //对于非数值类型
    val item2Num_analyseType_df=medicalCalculate.columnAnalyse(item2Num_df,"noNum")
//    println(item2Num_analyseType_df.filter(_.getString(3).equals("indeterminate")).count())
  }
}

class DataSchemaCalculate(spark: SparkSession) {
  def reduceData(all_result_df: DataFrame) ={



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

  import spark.implicits._



  def getDataDF(path: String, sep: String): DataFrame = {
    spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("sep", sep)
      .csv( path)

  }

  def columnAnalyse(item2Num_df:DataFrame,columnType:String):DataFrame={
    var item2Num_analyseType_df:DataFrame=spark.emptyDataFrame
    columnType match {
      case "Num" =>{
         item2Num_analyseType_df=item2Num_df.map{case Row(tableid:String,distinct_num:Int,sum_num:Int)=>
          var analyseType=""
          if(distinct_num<=distinct_threshold&&sum_num>sum_threshold){
            //当做离散变量
            analyseType="enum"
          }else {
            analyseType="numerical"
          }
          (tableid,distinct_num,sum_num,analyseType)
        }.toDF("tableid","distinct_num","sum_num","analyseType")
      }
      case "noNum" =>{
         item2Num_analyseType_df=item2Num_df.map{case Row(tableid:String,distinct_num:Int,sum_num:Int)=>
          var analyseType=""
          if(distinct_num<=distinct_threshold&&sum_num>sum_threshold){
            //当做离散变量
            analyseType="enum"
          }else if(distinct_num>distinct_threshold&&sum_num>sum_threshold){
            //当做NLP变量
            analyseType="nlp"
          }else if(distinct_num<=distinct_threshold&&sum_num<=sum_threshold){
            //看情况
            analyseType="indeterminate"
          }
          (tableid,distinct_num,sum_num,analyseType)
        }.toDF("tableid","distinct_num","sum_num","analyseType")
      }
    }
        item2Num_analyseType_df
  }


  }
