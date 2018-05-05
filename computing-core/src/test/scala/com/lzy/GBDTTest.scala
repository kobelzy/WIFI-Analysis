package com.lzy

import org.apache
import org.apache.spark
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.linalg
/**
  * Created by Administrator on 2018/5/5.
  */
object GBDTTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
import spark.implicits._
    val df: DataFrame = spark.read.format("libsvm").load("computing-core/src/test/resources/linear.txt")
      .map{case Row(label:Double,prediction:linalg.SparseVector)=>(Array(label,1.0),prediction)}.toDF("label","features")
    df.show(10, false)
    val df_split = df.randomSplit(Array(0.9, 0.1))
    val train = df_split(0)
    val test = df_split(1)
    val gbtr=new GBTRegressor()
      .setMaxIter(10)

    val evaluator = new RegressionEvaluator()
    evaluator.setMetricName("rmse")
    val model = gbtr.fit(train)
    val predictions = model.transform(test)
    predictions.select("label","prediction").show(10, false)
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
  }
}
