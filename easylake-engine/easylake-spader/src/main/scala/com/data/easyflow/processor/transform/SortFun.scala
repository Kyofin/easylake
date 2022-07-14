package com.data.easyflow.processor.transform

import com.data.easyflow.model.SortCol
import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class SortFun(context: JobContext,override val parameter: Config) extends TransFormProcessor {


  /**
   * [
   * {"colName":"id","sortMethod":"desc"},
   * {"colName":"name","sortMethod":"desc"}
   * ]
   */
  private val sortColumns = parameter.getList("sortColumns", classOf[SortCol])

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    val spark = context.spark
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val columns = sortColumns.map(s => {
      var column = col(s.getColName)
      if (s.getSortMethod == "desc") {
        column = column.desc
      } else {
        column = column.asc
      }
      column
    })
    var sortDF = inputData.defaultInput.get

    sortDF = sortDF.sort(columns: _*)
    sortDF

  }
}
