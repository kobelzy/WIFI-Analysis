package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/1/24.
  */
object TF_IDFTest {
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("name").master("local[*]").getOrCreate()
    val training = spark.createDataFrame(Seq(
      (0L, "请问 你是谁", 1.0)
//      (1L, "b d", 0.0),
//      (2L, "spark f g h", 1.0),
//      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")
    val test = spark.createDataFrame(Seq(
      (0L, "你是谁")
      //      (1L, "b d", 0.0),
      //      (2L, "spark f g h", 1.0),
      //      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text")
    val tokenizer=new Tokenizer().setInputCol("text").setOutputCol("words")
    val tokenized=tokenizer.transform(training)
    val hashingTF=new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("rawFeatures").setNumFeatures(20)
    val idf=new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val tfed=hashingTF.transform(tokenized)
    tfed.show()
//      .foreach(println(_))
    val idfed=idf.fit(tfed)
    idfed.transform(hashingTF.transform(tokenizer.transform(test)))show()
//      .select("features").foreach(println(_))
  }
}
