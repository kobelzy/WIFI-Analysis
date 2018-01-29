package com.lzy
import scala.collection.mutable.ArrayBuffer
/**
  * Created by Administrator on 2018/1/29.
  */
object ArrayTest {
  def main(args: Array[String]): Unit = {
    val arr=List[Int](1,2,3)
    arr:+10
    val newarr=arr.toBuffer+=10
    println("======")
    newarr.toList.foreach(println(_))

    val arr2=Array[Int](12,34,5)
    arr2(0)=10
    arr2.drop(0)
    arr2:+10
    println(arr2.mkString(","))
arr2.toBuffer
    val arr3=Array[List[Int]]()
  }
  def test(arr:ArrayBuffer[Int])={
    arr+=10
    arr+=20
  }
}
