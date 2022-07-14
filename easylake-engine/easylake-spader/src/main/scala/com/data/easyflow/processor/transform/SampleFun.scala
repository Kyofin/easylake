package com.data.easyflow.processor.transform

import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame

class SampleFun(context: JobContext,override val parameter: Config) extends TransFormProcessor {
  private val fraction = parameter.getDouble("fraction")


  override def checkParameter(): Unit = {
    if (fraction > 1 || fraction < 0) {
      throw new IllegalArgumentException(s"$fraction is now allow . fraction range [0.0, 1.0].")
    }
  }

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    var df = inputData.defaultInput.get
    df = df.sample(fraction)
    df
  }

}
