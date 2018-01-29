package com.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/1/24.
  */
object PipelineTrain {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("name").master("local[*]").getOrCreate()
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")
    val test=spark.createDataFrame(Seq((2L, "spark f g h"))).toDF("id", "text")
    val pipeline = new Pipeline()
    val tokenizer = new Tokenizer()
    val hashingTF = new HashingTF()
    val lr = new LogisticRegression()
    val paramMap = ParamMap(tokenizer.inputCol -> "text",
      tokenizer.outputCol -> "words",
      hashingTF.inputCol -> "words", hashingTF.outputCol -> "features", hashingTF.numFeatures -> 1000,
      lr.maxIter -> 10, lr.regParam -> 0.01
    )

    pipeline.setStages(Array(tokenizer, hashingTF, lr))
    val model:PipelineModel = pipeline.fit(training, paramMap)
model.transform(test).show()
    //model.write.overwrite().save("")
    //pipeline.write.overwrite().save("")
    //    val sameModel=PipelineModel.load("")

//    model.transform(training).show()
  }
}
