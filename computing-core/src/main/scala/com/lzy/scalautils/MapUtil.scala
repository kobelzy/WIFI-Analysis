package com.lzy.scalautils
import scala.collection.mutable
/**
  * Created by Administrator on 2018/1/1.
  */
object MapUtil {
  /**
    * 将list中的tupe写入map中，如果数据已经存在，将其值进行加和
    * * @param map
    * @param list
    * @return
    */
  def addMapListItem(map:mutable.Map[String,Int],list:List[(String,Int)]):mutable.Map[String,Int]={
    for(item<-list){
      if(map.contains(item._1)){
        map.update(item._1,map(item._1)+item._2)
        //map(item._1)=map(item._1)+item._2
      }else{
        map+=(item._1->item._2)
      }
    }
    map
  }
  def addMapListItemLog(map:mutable.Map[String,Long],list:List[(String,Long)]):mutable.Map[String,Long]={
    for(item<-list){
      if(map.contains(item._1)){
        map(item._1)=map(item._1)+item._2
      }else{
        map+=(item._1->item._2)
      }
    }
    map
  }

  def addMapItem(tmp:mutable.Map[String,Int],map:mutable.Map[String,Int]):mutable.Map[String,Int]={
for(item<-map){
  if(tmp.contains(item._1)){
    tmp(item._1)=tmp(item._1)+item._2
  }else{
    tmp+=(item._1->item._2)
  }
}
    tmp
  }
  def getMax10(map:mutable.Map[String,Int]):List[(String,Int)]={
    map.toList.sortWith(_._2>_._2).take(10)
  }
  def getMax(map:mutable.Map[String,Int]):List[(String,Int)]={
    map.toList.sortWith(_._2>_._2).take(1)
  }



}
