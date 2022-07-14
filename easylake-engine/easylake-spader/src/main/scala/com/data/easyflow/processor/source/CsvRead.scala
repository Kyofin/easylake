package com.data.easyflow.processor.source

import com.data.easyflow.processor.base.{JobContext, SourceProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvRead(context: JobContext, parameter: Config) extends SourceProcessor {
  private val path = parameter.getString("path")
  private val schema = parameter.getString("schema")
  private var df: DataFrame = _

  override def createDataFrame(spark: SparkSession): DataFrame = {
    df = spark.read.format("csv").option("path", path).schema(schema).load()
    df
  }


}
