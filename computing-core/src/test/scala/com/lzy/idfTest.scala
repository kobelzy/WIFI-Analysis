package com.lzy

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * Created by Administrator on 2018/5/3.
  */

object idfTest{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("medical")
      .getOrCreate()
    val sentenceData = spark.createDataFrame(Seq(
      (0, "（尿液）送检黄色液体约40ml，离心沉淀TCT制片一张，巴氏染色。镜下：见少量的尿路上皮细胞，未找到癌细胞。","离心沉淀TCT制片一张"),
      (0, " 中国 是 基建狂魔","离心沉淀TCT制片一张"),
      (1, "Logistic regression models","离心沉淀TCT制片一张")
    )).toDF("label", "sentence","result")
    val idftest=new idfTest(spark)
    val list=Array("sentence","result")
    var df=sentenceData
    for(tableid<-list){
      df=idftest.executeNLP(df,tableid)
    }
    df.show(false)
    df.printSchema()
    //(20000,[7282,9637,18665],[0.5753641449035617,0.6931471805599453,0.6931471805599453])
    //将tf结果转化为了TF-IDF值，可以看到，“美国”“伊拉克”只出现了一次，值相同，中国在所有文件中出现了两次，根据idf公式其值更小一些，如果中国在第三个文件中也出现，那么其值会变为0。这就是过滤掉常用词语保留重要词语的的意义所在
  }

}
class idfTest (spark:SparkSession){
  val stop = new StopRecognition()
  stop.insertStopNatures("w")//过滤掉标点
  def executeNLP(allResult_df: DataFrame, columnName:String) = {
    //分词
    val allResult_nlp_df =allResult_df.withColumn(columnName+"_nlp",str2NlpUDF(allResult_df(columnName)))
    //分为词组
    val tokenizer = new Tokenizer()
      .setInputCol(columnName+"_nlp")
      .setOutputCol(columnName+"_nlp_token")
    val allResult_nlp_token_df=tokenizer.transform(allResult_nlp_df)
    //计算词频
    val hashingTF = new HashingTF()
      .setInputCol(columnName+"_nlp_token").setOutputCol(columnName+"_nlp_token_tf")
      .setNumFeatures(1000)  //设置哈希桶的数量
    val allResult_nlp_token_tf_df=hashingTF.transform(allResult_nlp_token_df)
    //计算文档频率
    val idf = new IDF().setInputCol(columnName+"_nlp_token_tf").setOutputCol(columnName+"_nlp_token_tf_idf")
    val idfModel = idf.fit(allResult_nlp_token_tf_df)
    val rescaledData = idfModel.transform(allResult_nlp_token_tf_df)
    //删除之前计算过程中没有用的列。
    rescaledData.drop(columnName+"_nlp_token_tf",columnName+"_nlp_token",columnName+"_nlp",columnName)

  }


  val str2NlpUDF=udf((result:String)=>NlpAnalysis.parse(result).recognition(stop).toStringWithOutNature(" "),StringType)
}