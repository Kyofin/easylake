package com.data.easyflow.processor.source

import com.data.easyflow.model.TaskNode
import com.data.easyflow.processor.base.{JobContext, SourceProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class MysqlRead(context: JobContext, parameter: Config) extends SourceProcessor{
  private val url = parameter.getString("url")
  private val database = parameter.getString("database")
  private val table = parameter.getString("table")
  private val username = parameter.getString("username")
  private val password = parameter.getString("password")
  private var df: DataFrame = _

  override def createDataFrame(spark: SparkSession): DataFrame = {
    df= spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", url)
      .option("dbtable", s"$database.$table")
      .option("user", username)
      .option("password", password).load()
    df
  }
}
