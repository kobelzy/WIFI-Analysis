package com.lzy

import ml.dmlc.xgboost4j.scala.Booster
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**D:\WorkSpace\ScalaWorkSpace\WIFI-Analysis\computing-core\src\test\resources\linear.txt
  * Created by Administrator on 2018/3/8.
  */
object XGBoostTest {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf()
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Booster]))
    val spark=SparkSession.builder().master("local[*]").appName("xgboost")
        .config(sparkConf).getOrCreate()
    val df:DataFrame=spark.read.format("libsvm").load("computing-core/src/test/resources/linear.txt")
    val Array(trainDF,testDF):Array[Dataset[Row]]=df.randomSplit(Array(0.8,0.2))
    val params=Map( "eta" -> 0.1f,
      "max_depth" -> 2,
      "objective" -> "reg:linear")
    val xgboostModel=XGBoost.trainWithDataFrame(trainDF,params,10,3,useExternalMemory = true)
    xgboostModel.transform(testDF).show(false)
  }
}
