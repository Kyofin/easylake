package com.data.easyflow.processor.base

import com.data.easyflow.utils.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class JobContext(val spark: SparkSession) {
  val completedDataFramePool = new mutable.HashMap[String, DataFrame]()
  var isTryRun = false
}

trait SourceProcessor extends  Processor {
  def createDataFrame(spark: SparkSession): DataFrame
}

trait TransFormProcessor extends Processor {
  val parameter: Config

  def trans2DataFrame(inputData: InputData): DataFrame

  def cacheDataFrame(outputDF: DataFrame): DataFrame = {
    if (parameter.getBool("isCached",false)) {
      outputDF.cache()
    } else {
      outputDF
    }
  }

  def repartitionDataFrame(outputDF: DataFrame): DataFrame = {
    if (parameter.getBool("isRepartition",false)) {
     val repartitionNum = parameter.getInt("repartitionNum")
      outputDF.repartition(repartitionNum)
    } else {
      outputDF
    }
  }
}

trait SinkProcessor extends Processor {
  def save(dataFrame: DataFrame): Unit
}

trait Processor {

  def checkParameter(): Unit = {

  }
}

/**
 * 如果是单输入存储在defaultInput
 * 如果是多输入存储在multipleInput
 */
class InputData(val defaultInput :Option[DataFrame], val multipleInput :Option[Map[String,DataFrame]])




