package com.ml.kaggle

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Administrator on 2018/1/27.
  */
object Hource_price {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hourse_price")
      .master("local[*]")
      .getOrCreate()
    /**
      * 数据准备
      */
    val test = spark.read
      .option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .csv("F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture02_房价预测\\house price\\house_price_input\\test.csv")

    val train = spark.read
      .option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .csv("F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture02_房价预测\\house price\\house_price_input\\train.csv")
    //    train.show(10,truncate = false)
    //    test.show(10,truncate = false)
    import spark.implicits._
    val all_df: DataFrame = train.drop("SalePrice").union(test)
    /*al MSSubClass_new:DataFrame=all_df.select($"MSSubClass".cast(StringType))
//    MSSubClass_new.printSchema()
//    MSSubClass_new.show(10,false)
    val all_df2=all_df
      .withColumn("MSSubClass",MSSubClass_new.col("MSSubClass"))
    all_df2.printSchema()
    all_df2.show(10,false)*/
    //    all_df.show(10, truncate = false)
    //label,为了将其平滑，使用了log
    val y_train = train.select("SalePrice").map(row => Math.log1p(row.getAs[Int](0)))
    //        y_train.show(10, false)

    /**
      * 特征工程
      */
    val pipeLine = new Pipeline()
    val pipelineStages = Array[PipelineStage]()

    val col_stringType_List: List[String] = "MSSubClass" +: all_df.schema.toList.filter(_.dataType == StringType).map(_.name)
    // 或者   val col_stringType_List2:immutable.IndexedSeq[Any]="MSSubClass"+:all_df.dtypes.map(_._2).filter(_ == "StringType")
    println("String类型字段数：" + col_stringType_List.size)
    val col_numericerType_List: List[StructField] = all_df.schema.toList
      .filter(dataFeield => {
        dataFeield.dataType == IntegerType || dataFeield.dataType == DoubleType || dataFeield.dataType == LongType
      })

    println("数值类型字段数：" + col_numericerType_List.size)
    //    println(col_numericerType_List.map(_.name))
    val col_doubleType_List: List[StructField] = all_df.schema.toList.filter(_.dataType == DoubleType)
    println("Double类型字段数：" + col_doubleType_List.size)
    //1、使用均值替代空值
    //生成每一列的平均值
    val column2mean = getMeans(col_doubleType_List, all_df)
    println(column2mean)
    //循环包含所有数字类型的list，每次select其中的
    //    all_df.map(row=>{
    //
    //
    //
    //    })

    //2、使用独热编码来替代数据
    //MSSubClass是一个category列，但是被定义为了Integer,需要这里只是将其作为需要独热编码的字段。

    val indexed_pipelineStagesArray = takeStringIndexer(col_stringType_List, pipelineStages)
    val onehoted_pipelineStagesArray = takeOneHot(col_stringType_List, indexed_pipelineStagesArray)
    pipeLine.setStages(onehoted_pipelineStagesArray)
    val model = pipeLine.fit(all_df)
    val testRestul = model.transform(all_df.limit(20))
    testRestul.show(false)
    println(testRestul.columns.length)
    //3、标准化numerical类型数据

  }


  /**
    * 设置String转index，将df中所有的String类型进行转换
    *
    * @param cols
    * @param pipelineStages
    * @return
    */
  def takeStringIndexer(cols: List[String], pipelineStages: Array[PipelineStage]): Array[PipelineStage] = {
    val buffer = pipelineStages.toBuffer
    cols.foreach(col => {
      val indexerName = "str2Index_" + col
      val outputCol = col + "_indexed"
      val indexer = new StringIndexer().setInputCol(col).setOutputCol(outputCol)
        .setHandleInvalid("keep")
      buffer += indexer
    })
    buffer.toArray
  }

  def takeOneHot(cols: List[String], pipelineStages: Array[PipelineStage]): Array[PipelineStage] = {
    val buffer = pipelineStages.toBuffer
    cols.foreach(col => {
      val inputCol = col + "_indexed"
      val indexerName = "str2Index_" + col
      val outputCol = col + "_onehoted"
      val indexer = new OneHotEncoder().setInputCol(inputCol).setOutputCol(outputCol).setDropLast(false)
      buffer += indexer
    })
    buffer.toArray
  }

  /**
    * 返回指定DF中指定列的平均值，
    *
    * @param col_numericerType_List
    * @param all_df
    * @return Map[name, means]
    */
  def getMeans(col_numericerType_List: List[StructField], all_df: DataFrame): Map[String, Double] = {
    implicit val matchError = org.apache.spark.sql.Encoders.scalaDouble
    //想法：按照每一个给定的数据来求出其平均数
    val map = scala.collection.mutable.Map[String, Double]()
    col_numericerType_List.foreach(structField => {
      val name = structField.name
      val colDS: Dataset[Double] = structField.dataType.typeName match {
        case "integer" => all_df.select(name).map(row => row.getAs[Int](0).toDouble)
        case "double" => all_df.select(name).map(_.getAs[Double](0))
        case "long" => all_df.select(name).map(_.getAs[Long](0).toDouble)
      }
      val colCount = colDS
        .filter(!_.isNaN)
        .count()
      val mean = colDS.reduce(_ + _) / colCount
      map += (name -> mean)
    })
    map.toMap

  }
}
