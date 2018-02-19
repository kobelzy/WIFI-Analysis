package com.ml.kaggle

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml
import org.apache.spark.ml.{feature, linalg}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id

/**
  * Created by Administrator on 2018/2/14.
  */
object StoreSalesCompetition {


  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("storeSales")
      .master("local[*]")
      .getOrCreate()
    val goal = "Sales"
    val myid = "Id"
    val plot = true
    val path = "F:\\BaiduYunDownload\\Kaggle课程(关注公众号菜鸟要飞，免费领取200G+教程)\\Kaggle实战班(关注公众号菜鸟要飞，免费领取200G+教程)\\七月kaggle(关注公众号菜鸟要飞，免费领取200G+教程)\\代码(关注公众号菜鸟要飞，免费领取200G+教程)\\lecture07_销量预估\\data"

    val (train, test, features, featuresNonNumeric) = loadData(spark, path)
    train.show(10,false)
   val (trainByFill,testByFill)= processData(spark, train, test, features, featuresNonNumeric)
    features.filterNot(_=="Id").map(feature=>{
      (feature,trainByFill.filter( trainByFill(feature).isNull).count())
    }).foreach(println(_))

    features.filterNot(word=>word=="Sales"||word=="Customers").map(feature=>{
      (feature,testByFill.filter( testByFill(feature).isNull).count())
    }).foreach(println(_))


  }

  def loadData(spark: SparkSession, path: String): (DataFrame, DataFrame, Array[String], Array[String]) = {
    import spark.implicits._
    val read = spark.read.option("header", "true").option("nullValue", "NA").option("inferSchema", "true")

    val store = read.csv(path + "\\store.csv")
      .withColumn("Promo2", $"Promo2".cast(StringType))
      .withColumn("CompetitionOpenSinceMonth", $"CompetitionOpenSinceMonth".cast(StringType))
      .withColumn("CompetitionOpenSinceYear", $"CompetitionOpenSinceYear".cast(StringType))
      .withColumn("Promo2SinceWeek", $"Promo2SinceWeek".cast(StringType))
      .withColumn("Promo2SinceYear", $"Promo2SinceYear".cast(StringType))

    val train_org = read.csv(path + "\\train.csv")
      .withColumn("StateHoliday", $"StateHoliday".cast(StringType))
      .withColumn("DayOfWeek", $"DayOfWeek".cast(StringType))
      .withColumn("Open", $"Open".cast(StringType))
      .withColumn("Promo", $"Promo".cast(StringType))
      .withColumn("SchoolHoliday", $"SchoolHoliday".cast(StringType))

    //where build Join after,the Store will display two
    val train = train_org.join(store, Array("Store"), "left").withColumn("Id",monotonically_increasing_id())

    val test_org: DataFrame = read.csv(path + "\\test.csv")
      .withColumn("StateHoliday", $"StateHoliday".cast(StringType))
      .withColumn("DayOfWeek", $"DayOfWeek".cast(StringType))
      .withColumn("Open", $"Open".cast(StringType))
      .withColumn("Promo", $"Promo".cast(StringType))
      .withColumn("SchoolHoliday", $"SchoolHoliday".cast(StringType))
      .withColumn("Id", $"Id".cast(LongType))
    //    val test: DataFrame =test_org.join(store,test_org("Store")===store("Store"),"left")
    val test: DataFrame = test_org.join(store, Seq("Store"), "left")
    val features: Array[String] = train.columns
    val featuresNumeric: Seq[String] = test.schema.filter(line => line.dataType == IntegerType).map(_.name)
    val featuresNonNumeric = features.filterNot(line => featuresNumeric.contains(line))




    //Date,StateHoliday,StoreType,Assortment,PromoInterval
    (train, test, features, featuresNonNumeric)
  }

  case class store2VectorCase(Id: Long, promos: linalg.Vector)

  case class store2DateCase(Id: Long, year: String, month: String, day: String)

  def processData(spark: SparkSession, train: DataFrame, test: DataFrame, features: Array[String], featuresNonNumeric: Array[String]) = {
    val trainCleanSales = train.filter(train("Sales") > 0)
    //year month day process,promo interval
    val trainDF = processDateAndpromos(spark, trainCleanSales)
    println("train_long:"+trainCleanSales.count())
    println("trainDF_long:"+trainDF.count())

    val testDF = processDateAndpromos(spark, test)
    //Features set
    val noisyFeatures=Array("Id","Date")
   val features_drop_noisy= features.filterNot(noisyFeatures.contains(_))
    val featuresNonNumeric_drop_noisy=featuresNonNumeric.filterNot(noisyFeatures.contains(_))
    val fillMap=Map[String,Any]("Open"->"1",
      "CompetitionDistance"->0,
    "CompetitionOpenSinceMonth"->"0",
      "CompetitionOpenSinceYear"->"0",
      "Promo2SinceWeek"->"0",
      "Promo2SinceYear"->"0"
    )
    val trainByFill=trainDF.na.fill(fillMap)
    val testByFill=testDF.na.fill(fillMap)
    trainByFill.show(10,false)
    testByFill.show(10,truncate = false)
    (trainByFill,testByFill)
  }

  /**
    *
    * @param spark
    * @param data
    */
  def processDateAndpromos(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val store2DateDS: Dataset[store2DateCase] = data.select($"Id".as[Long], $"Date".as[String]).map { case (id, date) => {

val splites= date.split(" ")(0).split("-")
      val year = splites(0)
      val month = splites(1)
      val day = splites(2)
      store2DateCase(id, year, month, day)
    }
    }
    //January,February,March,April,May,June,July,August,September,October,November,December
    //Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sept,Oct,Nov,Dec
    val months = Array("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec")
    val promosDS = data.select($"Id".as[Long], $"PromoInterval".as[String])
      .map(line => {
        val intervalsStr: String = line._2
        if (intervalsStr != null) {
          val intervals = intervalsStr.split(",")
          //      val promos=new Array[Int](12)
          val promos = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
          intervals.foreach(month => {
            val index = months.indexOf(month)
            promos += Tuple2(index, 1.0)
          })
          store2VectorCase(line._1, linalg.Vectors.sparse(12, promos))
        } else {
          store2VectorCase(line._1,linalg.Vectors.dense(new Array[Double](12)))
        }
      })
    data
      .join(store2DateDS, Array("Id"), "left")
      .join(promosDS, Array("Id"), "left")
  }

}


/*
 |-- Store: integer (nullable = true)
 |-- DayOfWeek: string (nullable = true)
 |-- Date: timestamp (nullable = true)
 |-- Sales: integer (nullable = true)
 |-- Customers: integer (nullable = true)
 |-- Open: string (nullable = true)
 |-- Promo: string (nullable = true)
 |-- StateHoliday: string (nullable = true)
 |-- SchoolHoliday: string (nullable = true)
 |-- StoreType: string (nullable = true)
 |-- Assortment: string (nullable = true)
 |-- CompetitionDistance: integer (nullable = true)
 |-- CompetitionOpenSinceMonth: integer (nullable = true)
 |-- CompetitionOpenSinceYear: integer (nullable = true)
 |-- Promo2: string (nullable = true)
 |-- Promo2SinceWeek: integer (nullable = true)
 |-- Promo2SinceYear: integer (nullable = true)
 |-- PromoInterval: string (nullable = true)
* */