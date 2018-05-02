package com.ml.kaggle

import org.ansj.splitWord.analysis.{BaseAnalysis, NlpAnalysis, ToAnalysis}

/**
  * Created by Administrator on 2018/5/2.
  */
object AnsjTest {
  def main(args: Array[String]): Unit = {
    //基础分词
    val parse = BaseAnalysis.parse("我在艾泽拉斯")
//    基础分词不支持用户自定义词典，所以不发生改变
    println(parse)
    //精准分词
    val parse1 = ToAnalysis.parse("我在艾泽拉斯")
    println(parse1)
    //NLP分词
    val parse2 = NlpAnalysis.parse("我在艾泽拉斯")
    println(parse2)
  }
}
