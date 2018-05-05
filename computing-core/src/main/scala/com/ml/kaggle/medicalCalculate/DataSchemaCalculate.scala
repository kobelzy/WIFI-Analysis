package com.ml.kaggle.medicalCalculate

import com.ml.kaggle.medicalCalculate.DataSchemaCalculate.{dataCleanStr, distinct_threshold, sum_threshold}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, LinearRegression}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

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
    val item2Num_path = "E:\\dataset\\medicalCalculate\\classicNum\\item2Num.csv"
        val item2Num_distinct_path = "E:\\dataset\\medicalCalculate\\classicNum\\item2Num_distinct.csv"
    //hdfs
//    val base_path = "hdfs://master:9000/user/lzy/201805/"
//    val item2Num_path = "hdfs://master:9000/user/lzy/201805/item2Num.csv"
//    val item2Num_distinct_path = "hdfs://master:9000/user/lzy/201805/item2Num_distinct.csv"

    val allResult_path = "all_result.csv"
    val train_path = "train.csv"
    val test_path = "test.csv"


    val spark = SparkSession.builder()
            .master("local[*]")
      .appName("medical")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext.setLogLevel("WARN")

    val medicalCalculate = new DataSchemaCalculate(spark)

    val item2Num_df = medicalCalculate.getDataDF(item2Num_path, ",")
    val item2Num_distinct_df = medicalCalculate.getDataDF(item2Num_distinct_path, ",")

    val item2Num2Distinct_df: DataFrame = item2Num_df.join(item2Num_distinct_df, item2Num_df("table_id") === item2Num_distinct_df("table_id"))
      .map { case Row(tableid: String, num: Int, tableid2: String, ditinct: Int) =>
        (tableid, num, ditinct)
      }.toDF("table_id", "sum_num", "distinct_num")
      .filter(_.getInt(2) != 1)
    val allResult_df: DataFrame = medicalCalculate.getDataDF(base_path + allResult_path, "$")
    //
    val types: Array[(String, String)] = allResult_df.drop("vid").dtypes
    val (strTypes, numericalTypes) = types.partition(_._2.equals("StringType"))
    val item2Num_str_df = item2Num2Distinct_df.filter(row => strTypes.map(_._1).contains(row.getAs[String](0)))
    val item2Num_numerical_df = item2Num2Distinct_df.filter(row => numericalTypes.map(_._1).contains(row.getAs[String](0)))

    //对于非数值类型
    val item2Str_analyseType_df = medicalCalculate.columnAnalyseByScale(item2Num_str_df, "noNum")
    val item2Num_analyseType_df = medicalCalculate.columnAnalyseByScale(item2Num_numerical_df, "Num")
    //nlp需要进行nlp的数据
    val tableid_nlp_arr = item2Str_analyseType_df.filter(_.getString(3).equals("nlp")).select($"table_id".as[String]).collect()
    println("noNum:" + item2Str_analyseType_df.count())
    println("noNum-nlp:" + tableid_nlp_arr.length)
    var allResult_idf_df = allResult_df
    for (tableId <- tableid_nlp_arr) {
      allResult_idf_df = executeNLP(allResult_idf_df, tableId)
    }
    //    allResult_idf_df.show()

    //str的离散变量
    val tableid_str_enum_arr = item2Str_analyseType_df.filter(_.getString(3).equals("enum")).select($"table_id".as[String]).collect()
    //num的离散变量
    val tableid_num_enum_arr = item2Num_analyseType_df.filter(_.getString(3).equals("enum")).select($"table_id".as[String]).collect()
    val tableid_enum_arr: Array[String] = tableid_str_enum_arr ++ tableid_num_enum_arr
    println("noNum-str-enum:" + tableid_str_enum_arr.length)
    println("noNum-num-enum:" + tableid_num_enum_arr.length)
    println("noNum-enum:" + tableid_enum_arr.length)
    for (tableId <- tableid_enum_arr) {
      allResult_idf_df = executeEnum(allResult_idf_df, tableId)
    }
    //    allResult_idf_df.show()

    // 连续型变量
    val tableid_numerical_arr = item2Num_analyseType_df.filter(_.getString(3).equals("numerical")).select($"table_id".as[String]).collect()
    for (tableId <- tableid_numerical_arr) {
      allResult_idf_df = executeNumerical(allResult_idf_df, tableId)
    }

//    allResult_idf_df.write.parquet("hdfs://master:9000/user/lzy/201805/feature_result")
    val allFeatureTableId_arr = tableid_nlp_arr.map(_ + "_nlp_token_tf_idf") ++ tableid_enum_arr.map(_ + "_indexer_onehot") ++ tableid_numerical_arr.map(_ + "_scale")
    val selectFeature=new VectorAssembler()
      .setInputCols(allFeatureTableId_arr)
      .setOutputCol("features")

    val allResult_assamble_df=selectFeature.transform(allResult_idf_df)
        .select("vid","features")

    //
    val train_df = medicalCalculate.getDataDFOnStr(base_path + train_path, ",")
    val test_df = medicalCalculate.getDataDF(base_path + test_path, ",").select($"vid").toDF("vid_test")

    val cleanTrain_df = medicalCalculate.labeldataClean(train_df)

    val train2Feature_df = cleanTrain_df.join(allResult_assamble_df, cleanTrain_df("vid_train") === allResult_idf_df("vid"), "left")
    val test2Feature_df = test_df.join(allResult_assamble_df, cleanTrain_df("vid_test") === allResult_idf_df("vid"), "left")

    var result_df=test_df
    val aspects_arr=Array("c1", "c2", "c3", "c4", "c5")
    for(aspect<-aspects_arr){
      val train_features_df=train2Feature_df.select("vid","features",aspect).toDF("vid","features","label")
     val prediction_df= medicalCalculate.runGBDT(train_features_df,test2Feature_df).select("vid","prediction").toDF(aspect+"_vid",aspect)
      result_df=result_df.join(prediction_df,result_df("vid_test")===prediction_df(aspect+"_vid")).drop(aspect+"_vid")
    }
//    result_df.write.parquet("hdfs://master:9000/user/lzy/201805/result")
//result_df.rdd.saveAsTextFile("hdfs://master:9000/user/lzy/201805/result")
    result_df.show()
  }

  /**
    * 将需要进行nlp操作的的数据进行nlp，最终生成可以直接使用的向量。
    *
    * @param allResult_df
    * @param columnName
    * @return
    */
  def executeNLP(allResult_df: DataFrame, columnName: String): DataFrame = {
    //分词
    val allResult_nlp_df = allResult_df.withColumn(columnName + "_nlp", str2NlpUDF(allResult_df(columnName)))
    //分为词组
    val tokenizer = new Tokenizer()
      .setInputCol(columnName + "_nlp")
      .setOutputCol(columnName + "_nlp_token")
    val allResult_nlp_token_df = tokenizer.transform(allResult_nlp_df)
    //计算词频
    val hashingTF = new HashingTF()
      .setInputCol(columnName + "_nlp_token").setOutputCol(columnName + "_nlp_token_tf")
      .setNumFeatures(1000) //设置哈希桶的数量
    val allResult_nlp_token_tf_df = hashingTF.transform(allResult_nlp_token_df)
    //计算文档频率
    val idf = new IDF().setInputCol(columnName + "_nlp_token_tf").setOutputCol(columnName + "_nlp_token_tf_idf")
    val idfModel = idf.fit(allResult_nlp_token_tf_df)
    val rescaledData = idfModel.transform(allResult_nlp_token_tf_df)
    //删除之前计算过程中没有用的列。
    rescaledData.drop(columnName + "_nlp_token_tf", columnName + "_nlp_token", columnName + "_nlp", columnName)
  }

  val stop = new StopRecognition()
  stop.insertStopNatures("w")
  //过滤掉标点
  val str2NlpUDF: UserDefinedFunction = udf((result: String) => NlpAnalysis.parse(result).recognition(stop).toStringWithOutNature(" "), StringType)

  /**
    * 枚举变量转换
    *
    * @param allResult_df
    * @param columnName
    * @return
    */
  def executeEnum(allResult_df: DataFrame, columnName: String): DataFrame = {
    val indexer: StringIndexerModel = new StringIndexer()
      .setInputCol(columnName)
      .setOutputCol(columnName + "_indexer")
      .setHandleInvalid("keep")
      .fit(allResult_df)
    val allResult_indexed_df = indexer.transform(allResult_df)

    val encoder = new OneHotEncoder()
      .setInputCol(columnName + "_indexer")
      .setOutputCol(columnName + "_indexer_onehot")
    encoder.transform(allResult_indexed_df)
      .drop(columnName + "_indexer", columnName)
  }

  def executeNumerical(allResult_df: DataFrame, columnName: String): DataFrame = {
    val scaler = new MaxAbsScaler()
      .setInputCol(columnName)
      .setOutputCol(columnName + "_scale")

    val scalerModel = scaler.fit(allResult_df)
    val allResult_scale_df = scalerModel.transform(allResult_df)
    allResult_scale_df.drop(columnName)
  }


  /**
    * label之数据转换
    *
    * @param str
    * @return
    */
  def dataCleanStr(str: String): Double = {
    val replaceStr = str.replace(" ", "")
      .replace("+", "")
    var result = replaceStr
    if (replaceStr.split("\\.").length > 2) {
      val i = replaceStr.indexOf(".")
      val i2 = replaceStr.indexOf(".", 1)
      result = replaceStr.substring(0, i) + replaceStr.substring(i, i2)
    }
    if (result.contains("未做") || result.contains("未查") || result.contains("弃查")) {
      result = null
    }
    if (Try(result.toDouble).isFailure && result.length > 4) {
      result = result.substring(0, 4)
    }
    result.toDouble
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
      .csv(path).repartition(40)
  }

  def getDataDFOnStr(path: String, sep: String): DataFrame = {
    spark.read.option("header", "true")
      .option("nullValue", "NA")
      //      .option("inferSchema", "true")
      .option("sep", sep)
      .csv(path)
  }

  def columnAnalyseByScale(item2Num_df: DataFrame, columnType: String): DataFrame = {
    var item2Num_analyseType_df: DataFrame = spark.emptyDataFrame
    columnType match {
      case "Num" => {
        item2Num_analyseType_df = item2Num_df.map { case Row(tableid: String, distinct_num: Int, sum_num: Int) =>
          val scale = distinct_num.toDouble / sum_num.toDouble
          var analyseType = ""
          if (scale <= 1.1 && sum_num > 10) {
            //当做离散变量
            analyseType = "enum"
          } else {
            analyseType = "numerical"
          }
          (tableid, distinct_num, sum_num, analyseType)
        }.toDF("table_id", "distinct_num", "sum_num", "analyseType")
      }
      case "noNum"
      => {
        item2Num_analyseType_df = item2Num_df.map { case Row(tableid: String, distinct_num: Int, sum_num: Int) =>
          val scale = sum_num.toDouble / distinct_num.toDouble
          var analyseType = ""
          if (scale <= 1.1 && sum_num > 10) {
            //当做离散变量
            analyseType = "nlp"
          } else {
            //当做NLP变量
            analyseType = "enum"
            //          } else if (distinct_num <= distinct_threshold && sum_num <= sum_threshold) {
          }
          (tableid, distinct_num, sum_num, analyseType)
        }
          .toDF("table_id", "distinct_num", "sum_num", "analyseType")
      }
    }
    item2Num_analyseType_df
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
        }.toDF("table_id", "distinct_num", "sum_num", "analyseType")
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
            //          } else if (distinct_num <= distinct_threshold && sum_num <= sum_threshold) {
          } else {
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

  /**
    * label值得数据清洗
    *
    * @param train
    * @return
    */
  def labeldataClean(train: DataFrame) = {
    train.map { case Row(vid: String, inspect1: String, inspect2: String, inspect3: String, inspect4: String, inspect5: String) =>
      (vid, dataCleanStr(inspect1), dataCleanStr(inspect2), dataCleanStr(inspect3), dataCleanStr(inspect4), dataCleanStr(inspect5))
    }.toDF("vid_train", "c1", "c2", "c3", "c4", "c5")
  }

  def runGBDT(df:DataFrame,test:DataFrame)={
val gbdt=new GBTRegressor()
    .setFeaturesCol("features")
.setPredictionCol("prediction")
    val paramGrid=new ParamGridBuilder()
        .addGrid(gbdt.maxIter,Array(5,10,15))//迭代次数
        .addGrid(gbdt.maxDepth,Array(5,7,9))//树的最大深度
//        .addGrid(gbdt.minInfoGain,Array(0.0,0.1,0.3,0.5))//分裂节点时候的最小信息增益
//        .addGrid(gbdt.minInstancesPerNode,Array(1,2,3,4))//分裂后子节点包好最少的实例数量
//        .addGrid(gbdt.maxBins,Array(2,6,12,18,32))//连续特征离散化的最大数量，越大则分裂粒度越高
//        .addGrid(gbdt.lossType,Array("L1","L2"))
        .build()

//    val pipeline=new Pipeline()
//      .setStages()

    val trainValidationSplit=new TrainValidationSplit()
      .setEstimator(gbdt)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    val cvModel=trainValidationSplit.fit(df)
    val bestModel=cvModel.bestModel.asInstanceOf[GBTRegressionModel]

    val test_df=bestModel.transform(test)
    test_df
  }
}
