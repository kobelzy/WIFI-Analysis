package com

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:22 on 2018/5/15
  * Modified By：
  */
object DFJoinTest {
    case class keys(key1:Int,key2:Int,key3:Int)
    def main(args: Array[String]): Unit = {
        val spark=SparkSession.builder().appName("names")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

        val df1 = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6)) ).toDF("key1","key2","key3")
        val df2 = spark.createDataset(Seq(("aaa", 2, 2),    ("bbb", 3, 5),    ("ddd", 3, 5),    ("bbb", 4, 6), ("eee", 1, 2), ("aaa", 1, 5), ("fff",5,6))).toDF("key1","key2","key4")

//        val df3 = spark.createDataset(Seq((1, 1, 2), (2, 3, 4), (3, 3, 5), (2, 4, 6)) ).toDF("key1","key2","key3")
        val df3 = spark.createDataset(Seq((1, 1), (2, 3), (3, 3), (2, 4)) ).toDF("key1","key2")
        val df4 = spark.createDataset(Seq((1, 2, 2),    (2, 3, 5),    (4, 3, 5),    (2, 4, 6), (5, 1, 2), (1, 1, 5), (6,5,6))).toDF("key1","key2","key4")
df1.show()
        df2.show()
        df3.printSchema()
//        df1.join(df2,"key1").show()
        df4.join(df3,"key1").show()
//        df4.stat.freqItems(Array("key4")).show()
    }
}
