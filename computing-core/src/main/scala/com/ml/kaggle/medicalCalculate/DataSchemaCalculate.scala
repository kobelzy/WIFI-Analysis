package com.ml.kaggle.medicalCalculate

import com.ml.kaggle.AnsjTest.stop
import com.ml.kaggle.medicalCalculate.DataSchemaCalculate.{distinct_threshold, sum_threshold}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by Administrator on 2018/4/24.
  */

/** *
  * 病人体检样例中，每个病人的一项体检会使用一条来表示，每个病人有多个体检项目
  */
object DataSchemaCalculate {

  case class data(vid: String, table_id: String, field_results: String)

  case class id2dataInList(vid: String, list: Seq[data])

  val distinct_threshold = 5
  val sum_threshold = 20

  def main(args: Array[String]): Unit = {
    val base_path = "E:\\dataset\\medicalCalculate\\20180408\\"
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext.setLogLevel("WARN")

    val medicalCalculate = new DataSchemaCalculate(spark)
    val item2Num_path = "E:\\dataset\\medicalCalculate\\classicNum\\item2Num.csv"
    val item2Num_distinct_path = "E:\\dataset\\medicalCalculate\\classicNum\\item2Num_distinct.csv"
    val item2Num_df = medicalCalculate.getDataDF(item2Num_path, ",")
    val item2Num_distinct_df = medicalCalculate.getDataDF(item2Num_distinct_path, ",")

    val item2Num2Distinct_df: DataFrame = item2Num_df.join(item2Num_distinct_df, item2Num_df("table_id") === item2Num_distinct_df("table_id"))
      .map { case Row(tableid: String, num: Int, tableid2: String, ditinct: Int) =>
        (tableid, num, ditinct)
      }.toDF("table_id", "sum_num", "distinct_num")
      .filter(_.getInt(2) != 1)
    val allResult_path = "all_result.csv"
    val allResult_df = medicalCalculate.getDataDF(base_path + allResult_path, "$")
    //
    val types: Array[(String, String)] = allResult_df.drop("vid").dtypes
    val (strTypes, numericalTypes) = types.partition(_._2.equals("StringType"))
    val item2Num_str_df = item2Num2Distinct_df.filter(row => strTypes.map(_._1).contains(row.getAs[String](0)))
    val item2Num_numerical_df = item2Num2Distinct_df.filter(row => numericalTypes.map(_._1).contains(row.getAs[String](0)))

    //对于非数值类型
    val item2Str_analyseType_df = medicalCalculate.columnAnalyse(item2Num_str_df, "noNum")
    val item2Num_analyseType_df = medicalCalculate.columnAnalyse(item2Num_numerical_df, "Num")
    //如果需要对上百列特征做分词解析的话，那么需要需要传入一个columnName，DataSet来将其转换为新的数据类型。
    val tableid_nlp_arr=item2Str_analyseType_df.filter(_.getString(3).equals("nlp")).select($"table_id".as[String]).collect()
    println(tableid_nlp_arr.mkString(","))
    var allResult_idf_df=allResult_df
    for(tableId<-tableid_nlp_arr){
      allResult_idf_df=medicalCalculate.executeNLP(allResult_idf_df,tableId)
    }
    allResult_idf_df.show()
  }
}

class DataSchemaCalculate(spark: SparkSession) {
  import spark.implicits._

  def reduceData(all_result_df: DataFrame) = {


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



  def getDataDF(path: String, sep: String): DataFrame = {
    spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("sep", sep)
      .csv(path)

  }

  def columnAnalyse(item2Num_df: DataFrame, columnType: String): DataFrame = {
    var item2Num_analyseType_df: DataFrame = spark.emptyDataFrame
    columnType match {
      case "Num" => {
        item2Num_analyseType_df = item2Num_df.map { case Row(tableid: String, distinct_num: Int, sum_num: Int) =>
          var analyseType = ""
          if (distinct_num <= distinct_threshold && sum_num > sum_threshold) {
            //当做离散变量
            analyseType = "enum"
          } else {
            analyseType = "numerical"
          }
          (tableid, distinct_num, sum_num, analyseType)
        }.toDF("tableid", "distinct_num", "sum_num", "analyseType")
      }
      case "noNum" => {
        item2Num_analyseType_df = item2Num_df.map { case Row(tableid: String, distinct_num: Int, sum_num: Int) =>
          var analyseType = ""
          if (distinct_num <= distinct_threshold && sum_num > sum_threshold) {
            //当做离散变量
            analyseType = "enum"
          } else if (distinct_num > distinct_threshold && sum_num > sum_threshold) {
            //当做NLP变量
            analyseType = "nlp"
          } else if (distinct_num <= distinct_threshold && sum_num <= sum_threshold) {
            //看情况
            //            analyseType="indeterminate"
            analyseType = "enum"
          }
          (tableid, distinct_num, sum_num, analyseType)
        }
          .map { case (tableid, distinct_num, sum_num, analyseType) =>
            var new_analyseType = analyseType
            if (tableid.equals("21A059")) {
              new_analyseType = "nlp"
            }
            (tableid, distinct_num, sum_num, new_analyseType)
          }

          .toDF("table_id", "distinct_num", "sum_num", "analyseType")

      }
    }
    item2Num_analyseType_df
  }
  val stop = new StopRecognition()
  stop.insertStopNatures("w")//过滤掉标点
  /**
    * 将需要进行nlp操作的的数据进行nlp，最终生成可以直接使用的向量。
    * @param allResult_df
    * @param columnName
    * @return
    */
  def executeNLP(allResult_df: DataFrame, columnName:String): DataFrame = {
  //分词
   val allResult_nlp_df =allResult_df.withColumn(columnName+"_nlp",str2NlpUDF(allResult_df(columnName)))
    //分为词组
    val tokenizer = new Tokenizer()
      .setInputCol(columnName+"_nlp")
      .setOutputCol(columnName+"_nlp_token")
    val allResult_nlp_token_df=tokenizer.transform(allResult_nlp_df)
    //计算词频
    val hashingTF = new HashingTF()
      .setInputCol(columnName+"_nlp_token").setOutputCol(columnName+"_nlp_token_tf")
      .setNumFeatures(1000)  //设置哈希桶的数量
    val allResult_nlp_token_tf_df=hashingTF.transform(allResult_nlp_token_df)
    //计算文档频率
    val idf = new IDF().setInputCol(columnName+"_nlp_token_tf").setOutputCol(columnName+"_nlp_token_tf_idf")
    val idfModel = idf.fit(allResult_nlp_token_tf_df)
    val rescaledData = idfModel.transform(allResult_nlp_token_tf_df)
    //删除之前计算过程中没有用的列。
    rescaledData.drop(columnName+"_nlp_token_tf",columnName+"_nlp_token",columnName+"_nlp",columnName)
  }

  val str2NlpUDF=udf((result:String)=>NlpAnalysis.parse(result).recognition(stop).toStringWithOutNature(" "),StringType)

}
