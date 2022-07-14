package com.data.easyflow.processor.transform

import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame

class UnionFun(context: JobContext,override val parameter: Config) extends TransFormProcessor {

  private val mainTaskName = parameter.getString("mainTaskName")
  private val sideTaskName = parameter.getString("sideTaskName")
  private val unionType = parameter.getString("unionType")


  override def checkParameter(): Unit = {
    val allowedUnionType = Set("union", "unionAll")
    if (!allowedUnionType.contains(unionType)) {
      throw new IllegalArgumentException(s"$unionType is now allow . unionType only support union and union All.")
    }
  }

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    val multipleInput = inputData.multipleInput.get
    val mainDF = multipleInput(mainTaskName)
    val sideDF = multipleInput(sideTaskName)

    var df: Option[DataFrame] = None
    unionType match {
      case "union" =>
         df = Some(mainDF.union(sideDF).distinct())
      case "unionAll" =>
         df = Some(mainDF.union(sideDF))
    }
    df.get
  }
}
