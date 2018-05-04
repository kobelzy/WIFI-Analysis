package com.lzy

import org.apache
import org.apache.spark
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/4.
  */
object IndexTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.createDataFrame(Seq((0, null), (1, null), (2, "c"), (3, "a"), (4, "a"), (5, "c"), (6, "d"), (7, "d"))).toDF("id", "category")
    val indexer: StringIndexerModel = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
        .setHandleInvalid("keep")
      .fit(df)
    val indexed = indexer.transform(df)
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
    //      .setDropLast(false)
    val onehotdata=encoder.transform(indexed)
    onehotdata.show(false)


}
}
