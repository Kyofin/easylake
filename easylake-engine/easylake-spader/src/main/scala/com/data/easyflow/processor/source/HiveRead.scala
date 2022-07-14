package com.data.easyflow.processor.source

import com.data.easyflow.model.TaskNode
import com.data.easyflow.processor.base.{JobContext, SourceProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class HiveRead(context: JobContext, parameter: Config) extends SourceProcessor{
  private val dbName = parameter.getString("dbName")
  private val tbName = parameter.getString("tbName")
  private var df: DataFrame = _

  override def createDataFrame(spark: SparkSession): DataFrame = {
    df = spark.table(s"$dbName.$tbName")
    df
  }

}
