package com.ml.kaggle.medicalCalculate

import org.apache
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import TrainDataClean.{dataCleanStr}
import scala.util.Try

/**
  * Created by Administrator on 2018/5/5.
  */
object TrainDataClean{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
            .master("local[*]")
      .appName("medical")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext.setLogLevel("WARN")
    val train_path = "train.csv"
    val test_path = "test.csv"
    val trainDataClean=new TrainDataClean(spark)
    val train_df = trainDataClean.getDataDF(train_path, ",")
    val test_df = trainDataClean.getDataDF(test_path, ",").select($"vid").show()
    train_df.show(false)
    trainDataClean. labeldataClean(train_df).show(false)
  }


  /**
    * label之数据转换
    * @param str
    * @return
    */
  def dataCleanStr(str:String): Double ={
    val replaceStr= str.replace(" ","")
      .replace("+","")
    var result=replaceStr
    if(replaceStr.split("\\.").length>2){
      val i=replaceStr.indexOf(".")
      val i2=replaceStr.indexOf(".",1)
      result=replaceStr.substring(0,i)+replaceStr.substring(i,i2)
    }
    if(result.contains("未做")||result.contains("未查")||result.contains("弃查")){
      result=null
    }
    if(Try(result.toDouble).isFailure&&result.length>4){
      result=result.substring(0,4)
    }
    result.toDouble
  }
}
class TrainDataClean(spark:SparkSession) {
  import spark.implicits._
  val base_path = "E:\\dataset\\medicalCalculate\\20180408\\"

  def getDataDF(path: String, sep: String): DataFrame = {
    spark.read.option("header", "true")
      .option("nullValue", "NA")
//      .option("inferSchema", "true")
      .option("sep", sep)
      .csv(base_path + path)
  }

  /**
    * label值得数据清洗
    * @param train
    * @return
    */
  def labeldataClean(train:DataFrame)={
train.map{case Row(vid:String,inspect1:String,inspect2:String,inspect3:String,inspect4:String,inspect5:String)=>
  (vid,dataCleanStr(inspect1),dataCleanStr(inspect2),dataCleanStr(inspect3),dataCleanStr(inspect4),dataCleanStr(inspect5))
}.toDF("vid","c1","c2","c3","c4","c5")
  }

}

