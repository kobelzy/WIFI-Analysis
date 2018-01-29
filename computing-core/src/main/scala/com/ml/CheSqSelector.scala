package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/1/24.
  */
object CheSqSelector {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("name").master("local[*]").getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.createDataset(Seq(
      (1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1),
      (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0),
      (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0)
    )).toDF("id", "features", "label")
    val selector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val model = selector.fit(df)
    val result = model.transform(df)
    result.show(false)
    result.printSchema()


  }
}
