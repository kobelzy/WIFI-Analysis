package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/1/24.
  */
object VectorSlicer {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("name").master("local[*]").getOrCreate()
    //val df=spark.createDataFrame(Seq(Vectors.dense(-2.0,2.3,0.0))))
    //1、使用Array存储数据
    val data1 = Vectors.dense(-2.0, 2.3, 0.0)
    val df = spark.createDataFrame(Seq(Tuple1(data1))).toDF("userFeatures")
    //2、使用Lislt来装
    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val data2: ArrayBuffer[Row] = scala.collection.mutable.ArrayBuffer(Row(Vectors.dense(-2.0, 2.3, 0.0)))
    val list: java.util.List[Row] = data2
    val df2 = spark.createDataFrame(list, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer()
      .setInputCol("userFeatures").setOutputCol("features")
      .setIndices(Array(1))
          .setNames(Array("f3"))
    val output = slicer.transform(df2)
    output.show(false)
    output.printSchema()
  }
}
