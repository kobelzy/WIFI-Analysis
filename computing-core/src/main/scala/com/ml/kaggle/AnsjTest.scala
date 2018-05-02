package com.ml.kaggle

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.{BaseAnalysis, DicAnalysis, NlpAnalysis, ToAnalysis}
/**
  * Created by Administrator on 2018/5/2.
  */
object AnsjTest {
  val stop = new StopRecognition()
  stop.insertStopNatures("w")//过滤掉标点
  //    stop.insertStopNatures("m")//过滤掉m词性
  //    stop.insertStopNatures("null")//过滤null词性
      stop.insertStopNatures("<br />")//过滤<br　/>词性
      stop.insertStopNatures(":")
      stop.insertStopNatures("'")

  def main(args: Array[String]): Unit = {
    //基础分词
    val parse = BaseAnalysis.parse("大小、形态正常、实质回声均匀，胰管无扩张。CDFI：未见异常的血流信号。").recognition(stop).toStringWithOutNature("|")
//    基础分词不支持用户自定义词典，所以不发生改变
    println(parse)
    //精准分词
    val parse1 = ToAnalysis.parse("大小、形态正常、实质回声均匀，胰管无扩张。CDFI：未见异常血流信号。").recognition(stop).toStringWithOutNature("|")
    println(parse1)

    //NLP分词
    val parse2 = NlpAnalysis.parse("大小、形态正常、实质回声均匀，胰管无扩张。CDFI：未见异常血流信号。").recognition(stop).toStringWithOutNature(" ")
    println(parse2)
    val parse3=DicAnalysis.parse("大小、形态正常、实质回声均匀，胰管无扩张。CDFI：未见异常血流信号。").recognition(stop).toStringWithOutNature("|")
    println(parse3)
  }
}
