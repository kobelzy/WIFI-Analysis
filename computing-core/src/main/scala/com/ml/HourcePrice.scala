package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/1/27.
  */
object HourcePrice {
  Logger.getLogger("org.apache").setLevel(Level.WARN)
 /*
  |-- Id: integer (nullable = true)
 |-- MSSubClass: integer (nullable = true)
 |-- MSZoning: string (nullable = true)
 |-- LotFrontage: double (nullable = true)
 |-- LotArea: integer (nullable = true)
 |-- Street: string (nullable = true)
 |-- Alley: string (nullable = true)
 |-- LotShape: string (nullable = true)
 |-- LandContour: string (nullable = true)
 |-- Utilities: string (nullable = true)
 |-- LotConfig: string (nullable = true)
 |-- LandSlope: string (nullable = true)
 |-- Neighborhood: string (nullable = true)
 |-- Condition1: string (nullable = true)
 |-- Condition2: string (nullable = true)
 |-- BldgType: string (nullable = true)
 |-- HouseStyle: string (nullable = true)
 |-- OverallQual: integer (nullable = true)
 |-- OverallCond: integer (nullable = true)
 |-- YearBuilt: integer (nullable = true)
 |-- YearRemodAdd: integer (nullable = true)
 |-- RoofStyle: string (nullable = true)
 |-- RoofMatl: string (nullable = true)
 |-- Exterior1st: string (nullable = true)
 |-- Exterior2nd: string (nullable = true)
 |-- MasVnrType: string (nullable = true)
 |-- MasVnrArea: double (nullable = true)
 |-- ExterQual: string (nullable = true)
 |-- ExterCond: string (nullable = true)
 |-- Foundation: string (nullable = true)
 |-- BsmtQual: string (nullable = true)
 |-- BsmtCond: string (nullable = true)
 |-- BsmtExposure: string (nullable = true)
 |-- BsmtFinType1: string (nullable = true)
 |-- BsmtFinSF1: integer (nullable = true)
 |-- BsmtFinType2: string (nullable = true)
 |-- BsmtFinSF2: integer (nullable = true)
 |-- BsmtUnfSF: integer (nullable = true)
 |-- TotalBsmtSF: integer (nullable = true)
 |-- Heating: string (nullable = true)
 |-- HeatingQC: string (nullable = true)
 |-- CentralAir: string (nullable = true)
 |-- Electrical: string (nullable = true)
 |-- 1stFlrSF: integer (nullable = true)
 |-- 2ndFlrSF: integer (nullable = true)
 |-- LowQualFinSF: integer (nullable = true)
 |-- GrLivArea: integer (nullable = true)
 |-- BsmtFullBath: integer (nullable = true)
 |-- BsmtHalfBath: integer (nullable = true)
 |-- FullBath: integer (nullable = true)
 |-- HalfBath: integer (nullable = true)
 |-- BedroomAbvGr: integer (nullable = true)
 |-- KitchenAbvGr: integer (nullable = true)
 |-- KitchenQual: string (nullable = true)
 |-- TotRmsAbvGrd: integer (nullable = true)
 |-- Functional: string (nullable = true)
 |-- Fireplaces: integer (nullable = true)
 |-- FireplaceQu: string (nullable = true)
 |-- GarageType: string (nullable = true)
 |-- GarageYrBlt: double (nullable = true)
 |-- GarageFinish: string (nullable = true)
 |-- GarageCars: integer (nullable = true)
 |-- GarageArea: integer (nullable = true)
 |-- GarageQual: string (nullable = true)
 |-- GarageCond: string (nullable = true)
 |-- PavedDrive: string (nullable = true)
 |-- WoodDeckSF: integer (nullable = true)
 |-- OpenPorchSF: integer (nullable = true)
 |-- EnclosedPorch: integer (nullable = true)
 |-- 3SsnPorch: integer (nullable = true)
 |-- ScreenPorch: integer (nullable = true)
 |-- PoolArea: integer (nullable = true)
 |-- PoolQC: string (nullable = true)
 |-- Fence: string (nullable = true)
 |-- MiscFeature: string (nullable = true)
 |-- MiscVal: integer (nullable = true)
 |-- MoSold: integer (nullable = true)
 |-- YrSold: integer (nullable = true)
 |-- SaleType: string (nullable = true)
 |-- SaleCondition: string (nullable = true)
 |-- SalePrice: integer (nullable = true)

*/
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hourse_price")
      .master("local[*]")
      .getOrCreate()
    /**
      *数据准备
      */
    val train: DataFrame = spark.read.format("csv").load("F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture02_房价预测\\house price\\house_price_input\\train.csv")
    val test = spark.read.csv("F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture02_房价预测\\house price\\house_price_input\\test.csv")
    //    train.show(10,truncate = false)
    //    test.show(10,truncate = false)
    //构建DF格式
    import spark.implicits._
    val schema_name_list: Array[String] = test.filter(_.getAs[String](0) == "Id").map(row => row.mkString(",")).collect()(0).split(",")
    val schema_structFields: Array[StructField] = schema_name_list.map(name => {
      StructField(name, StringType, nullable = true)
    })
    val schemaStuct = StructType(schema_structFields)


    //和饼干数据统一处理，并删除index行
    val all = train.drop("_c80").union(test).filter(_.getAs[String](0) != "Id").rdd
    val all_df: DataFrame = spark.createDataFrame(all, schemaStuct)
    val test_df=all_df.limit(10)
//    all_df.show(10, truncate = false)
    //label,为了将其平滑，使用了log
    val y_train = train.select("_c80").filter(_.getAs[String](0)!= "SalePrice").map(row => Math.log1p(row.getAs[String](0).toDouble)).toDF("SalePrice")
//    y_train.show(10, false)
    /**
      * 特征工程
      */
      val stringIndexer=new StringIndexer().setInputCol("MSSubClass").setOutputCol("MSSubClass_indexed")
    val oneHot=new OneHotEncoder().setInputCol("MSSubClass_indexed").setOutputCol("MSSubClass_").setDropLast(false)
    val transformArray=Array(stringIndexer,oneHot)
val pipeLine=new Pipeline().setStages(transformArray)
    val model=pipeLine.fit(all_df)
    model.transform(test_df).show(false)
  }
}
