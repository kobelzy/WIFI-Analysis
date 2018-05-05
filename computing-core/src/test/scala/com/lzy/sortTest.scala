package com.lzy

/**
  * Created by Administrator on 2018/4/27.
  */
object sortTest {
  def main(args: Array[String]): Unit = {
    //contains
    val str=".+a.b,c|d&e$f"
    println(str.contains("."))
    println(str.contains(","))
    println(str.contains("|"))
    println(str.contains("&"))
    println(str.contains("$"))
    println(str.replace(".",""))
    println(str.replace(",",""))
    println(str.replace("|",""))
    println(str.replace("&",""))
    println(str.replace("+",""))


    println(str.split("\\.").length)
    println(str.split("\\,").length)
    println(str.split("\\|").length)
    println(str.split("\\&").length)
    println(str.split("\\$").length)
println("   ")

    println(str.indexOf("."))
    println(str.indexOf(","))
    println(str.indexOf("|"))
    println(str.indexOf("&"))
    println(str.indexOf("$"))
    println(str.indexOf("\\."))
    println(str.indexOf("\\,"))
    println(str.indexOf("\\|"))
    println(str.indexOf("\\&"))
    println(str.indexOf("\\$"))

    val st:String=null
println(st.toDouble)
/*    val list=List[Int](1,5,10,22,8)
//    val list=List[Int](1)
    list.distinct.sorted
          .sliding(2)
        .map(line=>
          (line.head,line.last)
        ).zipWithIndex.map(tuple=>(tuple._1,tuple._2+1))
      .foreach(println)
    println(10.0/3.0)
    println("sjfid<".contains("\\<"))*/
  }
}
