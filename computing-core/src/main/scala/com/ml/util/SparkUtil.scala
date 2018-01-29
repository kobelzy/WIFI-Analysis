package com.ml.util

import org.apache.spark.sql.Row

/**
  * Created by Administrator on 2018/1/27.
  */
object SparkUtil {
  def getN(row: Row, index: Int) = {
    if (row.isNullAt(index)) {

    } else {
      row.getAs(index)
    }

  }
}
