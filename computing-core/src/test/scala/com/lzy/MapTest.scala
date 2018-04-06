package com.lzy

import shapeless.PolyDefns.->
import scala.collection.mutable
/**
  * Created by Administrator on 2018/4/1.
  */
object MapTest {
  def main(args: Array[String]): Unit = {
    var map=Map[String,Map[String,Int]]()
    val list=List(("1","11",111),("1","12",122),("2","22",222),("3","33",333))
    for((fieldName,key1,value1)<-list){
      map.get(fieldName) match {
        case None =>{
          val innerMap=Map[String,Int](key1->value1)
          map+=(fieldName->innerMap)}
        case Some(innerMap:Map[String,Int])=>{
          //跟新当前的map值
         val innersMap= innerMap+(key1->value1)
          map+=(fieldName->innersMap)
        }
      }
    }
map.foreach(println)

    println("使用动态Map")
    val mutableMap=mutable.Map[String,mutable.Map[String,Int]]()
    for((fieldName,key1,value1)<-list) {
    //  mutableMap.get(fieldName) match {
      //  case None =>mutableMap(fieldName)=mutable.Map[String,Int](key1->value1)
        //case Some(innerMap)=>          innerMap(key1)=value1

      //}
      mutableMap.getOrElseUpdate(fieldName,mutable.Map[String,Int](key1->value1))
          .+=(key1->value1)
    }
      mutableMap
        .mapValues(_.toMap).toMap
        .foreach(println)

    }
}
