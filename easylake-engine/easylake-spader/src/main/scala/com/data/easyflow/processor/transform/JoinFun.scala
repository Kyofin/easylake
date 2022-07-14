package com.data.easyflow.processor.transform

import com.data.easyflow.model.JoinCol
import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

class JoinFun(context: JobContext, override val parameter: Config) extends TransFormProcessor {
  private val mainTaskName = parameter.getString("mainTaskName")
  private val sideTaskName = parameter.getString("sideTaskName")
  private val joinType = parameter.getString("joinType")

  private val joinCols = parameter.getList("joinCols", classOf[JoinCol])




  override def checkParameter(): Unit = {
    val allowedJoinType = Set("inner", "cross", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")
    if (!allowedJoinType.contains(joinType)) {
      throw new IllegalArgumentException(s"$joinType is now allow . joinType only support    `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`, `right`, `right_outer`, `left_semi`, `left_anti`.")
    }
  }

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    val multipleInput = inputData.multipleInput.get
    val mainDF = multipleInput(mainTaskName)
    val sideDF = multipleInput(sideTaskName)
    //todo 如果左右表join的字段同名？
    val condition = joinCols.map(j => s"${j.getMainColName}=${j.getSideColName}").mkString(" and ")

    val df = mainDF.join(sideDF, expr(condition), joinType)
    df
  }
}
