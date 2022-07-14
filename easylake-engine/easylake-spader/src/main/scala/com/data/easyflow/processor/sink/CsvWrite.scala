package com.data.easyflow.processor.sink

import com.data.easyflow.processor.base.{JobContext, SinkProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame

class CsvWrite(context: JobContext, parameter: Config)  extends SinkProcessor  {
  private val path = parameter.getString("path")
  private val header = parameter.getString("header")
  private val mode = parameter.getString("mode")
  override def save(dataFrame: DataFrame): Unit = {
    dataFrame.write.format("csv")
      .option("path",path)
      .option("header",header)
      .mode(mode)
      .save()
  }
}
