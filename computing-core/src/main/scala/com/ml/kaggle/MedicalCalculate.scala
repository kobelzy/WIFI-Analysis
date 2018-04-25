package com.ml.kaggle

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, SparkSession}

/**
  * Created by Administrator on 2018/4/24.
  */

/***
  * 病人体检样例中，每个病人的一项体检会使用一条来表示，每个病人有多个体检项目
  */
object MedicalCalculate{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
    val sc=spark.sparkContext.setLogLevel("warn")

    val medicalCalculate=new MedicalCalculate(spark)
    val data1_path="data_part1.txt"//3,673,450   ggg根据via去重之后 57298
    val data2_path="data_part2.txt"//3,673,450   根据via去重之后 57298
    val train_path="train.csv"
    val test_path="test.csv"
    val data1_df=medicalCalculate.getDataDF(data2_path,"$")
    val data2_df=medicalCalculate.getDataDF(data2_path,"$")
    val train_df=medicalCalculate.getDataDF(train_path,",")
    val test_df=medicalCalculate.getDataDF(test_path,",")
    import spark.implicits._
//    val data_df=data1_df.join(data2_df,"vid")
//    val data_df1=data1_df.groupByKey(_.getAs[String](0)).count()
//    val data_df2=data1_df.groupByKey(_.getAs[String](0)).count()
//
//    println(data_df1.count())
//    println(data_df2.count())
//    println(data1_df.count())
//    print(data2_df.count())
val reduceData=medicalCalculate.reduceData(data1_df,data2_df)
    reduceData.show(false)


  }

}
class MedicalCalculate(spark:SparkSession) {
  /**
    * 特征工程部分
    * 1、异常值处理
    * 2、缺失值处理
    * 3、数据标准化
    * 4、特征编码转换
    * 5、特征选择
    *
    *
    */

import spark.implicits._
  val base_path="E:\\dataset\\medicalCalculate\\20180408\\"
def getDataDF(path:String,sep:String): DataFrame ={
  spark.read.option("header","true")
    .option("nullValue","NA")
    .option("inferSchema","true")
      .option("sep",sep)
.csv(base_path+path)
}

  /**
    * 特征工程
    */
  /*
  0、实际场景预处理
   */
  def reduceData(data1_df:DataFrame,data2_df:DataFrame)={
    //首先，将每个用户变为一行，每一个特征作为一列
    data1_df.groupByKey(_.getString(0))
            .mapGroups((via,iter)=>{
              (via,iter.map(_.getAs[String](0)))
            }).toDF("via","features")


  }




/*
1、异常值处理
 */


}
