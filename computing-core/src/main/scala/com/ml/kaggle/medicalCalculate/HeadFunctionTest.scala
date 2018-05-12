package com.ml.kaggle.medicalCalculate

/**
  * Created by Administrator on 2018/5/12.
  */
object HeadFunctionTest {
  val f1 = (x:Int) => x+1
  def f2 = (_:Int)+1
  def main(args: Array[String]): Unit = {
    println(f1)
    println(f1(2))
    println(f1(3))

    println(f2(2))
    println(f2(3))
    println("helloworld")
  }
}
