package com.lzy

/**
  * Created by Administrator on 2018/4/27.
  */
object sortTest {
  def main(args: Array[String]): Unit = {
    val list=List[Int](1,5,10,22,8)
//    val list=List[Int](1)
    list.distinct.sorted
          .sliding(2)
        .map(line=>
          (line.head,line.last)
        ).zipWithIndex.map(tuple=>((tuple._1._1,tuple._1._2),tuple._2+1))
      .foreach(println)
  }
}
