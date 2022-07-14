package com.data.easyflow.processor.utils

import org.apache.spark.sql.SparkSession

object SparkSessionUtil {
  private var _spark: SparkSession = _

  def initSparkSession() = {
    // 初始化spark上下文
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.scheduler.mode","FAIR")
      .config("spark.scheduler.allocation.file","/Volumes/Samsung_T5/opensource/easyflow/src/main/resources/fairscheduler.xml")
      .getOrCreate()
    _spark = spark
  }

  def getSparkSession() = {
    _spark
  }

  def closeSparkSession() = {
    _spark.close()
  }
}
