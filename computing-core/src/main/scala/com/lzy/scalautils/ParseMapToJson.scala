package com.lzy.scalautils

import net.minidev.json.JSONObject
import scala.collection.JavaConversions.mutableMapAsJavaMap //用于sccalamap对于javamap的隐式转换
import scala.collection.mutable
/**
  * Created by Administrator on 2018/1/1.
  */
object ParseMapToJson {
  def map2Json(map:mutable.Map[String,Int]):String={
    val jsonString=JSONObject.toJSONString(map)
    jsonString
  }
  def map2Json2(map:mutable.Map[String,Long]):String={
    val jsonString=JSONObject.toJSONString(map)
    jsonString
  }
  def map2JsonList(list:List[(String,Int)]):String={
    var map:mutable.Map[String,Int]=mutable.Map()
    for(item<-list){
      map+=(item._1->item._2)
    }
    map2Json(map)
  }
}
