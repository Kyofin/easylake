package com.data.easyflow.processor.transform

import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame

class FilterFun(context: JobContext, override val parameter: Config) extends TransFormProcessor {
  private val condition = parameter.getString("condition")

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    var df = inputData.defaultInput.get
    df = df.filter(condition)
    df
  }
}
