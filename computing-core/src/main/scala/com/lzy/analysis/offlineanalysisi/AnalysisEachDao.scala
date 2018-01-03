package com.lzy.analysis.offlineanalysisi


import com.lzy.common.constants.AnalysisConstants
import com.lzy.javautils.ParseJson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2018/1/2.
  */
object AnalysisEachDao {
  def analysisStayTime(sc: SparkContext, conf: Configuration) = {
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    val count = hBaseRDD.count()
    hBaseRDD.foreach { case (_, result) => {
      val dataList = Bytes.toString(result.getValue(AnalysisConstants.DATA_CF, AnalysisConstants.dataList))
      val macList = ParseJson.jsonToList(dataList)
      val probeId = Bytes.toString(result.getValue(AnalysisConstants.PROBEINFO_CF, AnalysisConstants.probe_id))
      val mmac = Bytes.toString(result.getValue(AnalysisConstants.PROBEINFO_CF, AnalysisConstants.mmac))
      val rate = Bytes.toString(result.getValue(AnalysisConstants.PROBEINFO_CF, AnalysisConstants.rate))
      val record_time = Bytes.toString(result.getValue(AnalysisConstants.PROBEINFO_CF, AnalysisConstants.record_time))
      val wmac = Bytes.toString(result.getValue(AnalysisConstants.PROBEINFO_CF, AnalysisConstants.wmac))
      val addr = Bytes.toString(result.getValue(AnalysisConstants.ADDRESS_CF, AnalysisConstants.addr))
      val dataIterator = macList.iterator()
      while (dataIterator.hasNext) {
        //print(dataIterator.next().getMac()
      }
      //    println("RDDCount:"++count)

    }
    }
  }
}
