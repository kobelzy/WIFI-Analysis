package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/1/24.
  */
object RFormual {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("name").master("local[*]").getOrCreate()
    val df = spark.createDataFrame(Seq((7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0))).toDF("id", "country", "hour", "clicked")
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setForceIndexLabel(true)
    val output = formula.fit(df)
    val result = output.transform(df)
    result.show(true)
    result.printSchema()


  }
}
