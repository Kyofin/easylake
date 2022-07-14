package com.data.easyflow.processor.transform

import com.data.easyflow.model.{ExpressionCol, SchemaChangeCol}
import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

/**
 * 用于schema调整：如字段名修改，字段类型修改
 */
class SchemaChangeFun(context: JobContext, override val parameter: Config) extends TransFormProcessor {

  /**
   * [
   * {"originalColName":"id","changedColName":"student_id","isKeep":true,"changedColType":"double"}
   * {"originalColName":"name","isKeep":false}
   * ]
   */
  private val schemaChangeCols = parameter.getList("schemaChangeCols", classOf[SchemaChangeCol])

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    var df = inputData.defaultInput.get
    // 删除不需要的字段
    val dropCols = schemaChangeCols.filter(!_.isKeep).map(_.getOriginalColName)
    df = df.drop(dropCols: _*)

    schemaChangeCols.filter(_.isKeep).foreach(sc => {
      // 改字段类型
      df = df.withColumn(sc.getOriginalColName, df.col(sc.getOriginalColName).cast(sc.getChangedColType))
      // 改字段名
      df = df.withColumnRenamed(sc.getOriginalColName, sc.getChangedColName)
    })

    df.printSchema()
    df.show()
    df
  }
}
