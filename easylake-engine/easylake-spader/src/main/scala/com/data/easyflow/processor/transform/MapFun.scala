package com.data.easyflow.processor.transform

import com.data.easyflow.model.ExpressionCol
import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

/**
 * 用于字段值转换
 */
class MapFun(context: JobContext,override val parameter: Config) extends TransFormProcessor {

  /**
   * [
   *  {"colName":"id","expression":"lower(id)"},
   *  {"colName":"name","expression":"upper(name)"}
   * ]
   */
  private val expressions = parameter.getList("expressions",classOf[ExpressionCol])

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    var df = inputData.defaultInput.get
    expressions.foreach(e=>{
      df = df.withColumn(e.getColName,expr(e.getExpression))
    })
    df
  }
}
