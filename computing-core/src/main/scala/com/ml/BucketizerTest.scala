package com.ml

import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/4/15.
  */
object BucketizerTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("bucketizer").master("local[*]").getOrCreate()
    import spark.implicits._
    val splits = Array(Double.NegativeInfinity, -100, -10, 0.0, 10, 90, Double.PositiveInfinity)
    val data: Array[Double] = Array(-180,-160,-100,-50,-70,-20,-8,-5,-3, 0.0, 1,3,7,10,30,60,90,100,120,150)


    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")


//    val bucketizer = new Bucketizer().setInputCol("features").setOutputCol("bucketedFeatures").setSplits(splits)
//    val bucketedData = bucketizer.transform(dataFrame)
//
//
//    bucketedData.show(50,truncate=false)
//    bucketedData.filter($"bucketedFeatures"<=1).show(50,false)



    val discretizer=new QuantileDiscretizer()
      .setInputCol("features")
      .setOutputCol("result")
      .setNumBuckets(4)
    val model=discretizer.fit(dataFrame)
    val result=model.transform(dataFrame)
    result.show(false)
    model.getSplits.foreach(println)
    val splitsQ=model.getSplits
    val Q1=splitsQ(1)
    val Q3=splitsQ(3)
    println(Q1)
    val max_Whisker=Q3+(Q3-Q1)*1.5
    val min_Whisker=Q1-(Q3-Q1)*1.5
    val resultData=dataFrame.filter(row=>{
      val feature=row.getAs[Double]("features")
      feature>=min_Whisker && feature<=max_Whisker
    })
    resultData.show(false)
  }
}
