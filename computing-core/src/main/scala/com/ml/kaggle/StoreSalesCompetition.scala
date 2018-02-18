package com.ml.kaggle

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    processData(spark, train, test, features, featuresNonNumeric)
        train.show(10,false)
        train.printSchema()
    //    test.show(10,false)


    println(features.mkString(","))
    println("Non")
    println(featuresNonNumeric.mkString(","))

  }

  def loadData(spark: SparkSession, path: String): (DataFrame, DataFrame, Array[String], Array[String]) = {
    import spark.implicits._
    val read = spark.read.option("header", "true").option("nullValue", "NA").option("inferSchema", "true")

    val store = read.csv(path + "\\store.csv")
    val train_org = read.csv(path + "\\train.csv")
      .withColumn("StateHoliday", $"StateHoliday".cast(StringType))
      .withColumn("DayOfWeek", $"DayOfWeek".cast(StringType))
      .withColumn("Open", $"Open".cast(StringType))
      .withColumn("Promo", $"Promo".cast(StringType))
      .withColumn("SchoolHoliday", $"SchoolHoliday".cast(StringType))
      .withColumn("Promo2", $"Promo2".cast(StringType))
      .withColumn("SchoolHoliday", $"SchoolHoliday".cast(StringType))
      .withColumn("SchoolHoliday", $"SchoolHoliday".cast(StringType))
    //where build Join after,the Store will display two
    val train = train_org.join(store, Array("Store"), "left")

    train.printSchema()
    val test_org: DataFrame = read.csv(path + "\\test.csv")
      .withColumn("StateHoliday", $"StateHoliday".cast(StringType))
    //    val test: DataFrame =test_org.join(store,test_org("Store")===store("Store"),"left")
    val test: DataFrame = test_org.join(store, Seq("Store"), "left")
    test.select("Store").show(10,false)
    val features: Array[String] = test.columns
    val featuresNumeric: Seq[String] = test.schema.filter(line => line.dataType == IntegerType).map(_.name)
    val featuresNonNumeric = features.filterNot(line => featuresNumeric.contains(line))

     val nullFeatures= featuresNumeric.filterNot(_=="Id").map(feature=>{
      (feature,test.filter( test(feature).isNull).count())
    })
    nullFeatures.foreach(println(_))
    //Date,StateHoliday,StoreType,Assortment,PromoInterval
    (train, test, features, featuresNonNumeric)
  }

  case class store2VectorCase(Store: Int, promos: linalg.Vector)

  case class store2DateCase(Store: Int, year: Double, month: Double, day: Double)

  def processData(spark: SparkSession, train: DataFrame, test: DataFrame, features: Array[String], featuresNonNumeric: Array[String]) = {
    val trainCleanSales = train.filter(train("Sales") > 0)
    //year month day process,promo interval
    val trainDF = processDateAndpromos(spark, trainCleanSales)
    val testDF = processDateAndpromos(spark, test)
    //Features set
    val noisyFeatures=Array("Id","Date")
   val features_drop_noisy= features.filterNot(noisyFeatures.contains(_))
    val featuresNonNumeric_drop_noisy=featuresNonNumeric.filterNot(noisyFeatures.contains(_))
    val fillMap=scala.collection.mutable.Map[String,Any]()
    fillMap+=("Open"->1)
  }

  /**
    *
    * @param spark
    * @param data
    */
  def processDateAndpromos(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._
    val store2DateDS: Dataset[store2DateCase] = data.select($"Store".as[Int], $"Date".as[String]).map { case (store, date) => {
      val year = date.split(" ")(0).split("-")(0).toDouble
      val month = date.split(" ")(0).split("-")(1).toDouble
      val day = date.split(" ")(0).split("-")(2).toDouble
      store2DateCase(store, year, month, day)
    }
    }

    //January,February,March,April,May,June,July,August,September,October,November,December
    //Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sept,Oct,Nov,Dec
    val months = Array("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec")
    val promosDS = data.select($"Store".as[Int], $"PromoInterval".as[String])
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
          store2VectorCase(line._1, null)
        }
      })
    data.join(store2DateDS, Array("Store"), "left").join(promosDS, Array("Store"), "left")
  }

}
