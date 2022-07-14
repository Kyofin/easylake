package com.data.easyflow.processor.transform

import com.data.easyflow.model.AggCol
import com.data.easyflow.processor.base.{InputData, JobContext, TransFormProcessor}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

class AggregatorFun(context: JobContext, override val parameter: Config) extends TransFormProcessor {


  /**
   * ["name","id"]
   */
  private val groupList = parameter.getList("groupList", classOf[String])

  /**
   * [
   * {"colName":"id","aggMethod":"avg"},
   * {"colName":"name","aggMethod":"max"}
   * ]
   */
  private val aggList = parameter.getList("aggList", classOf[AggCol])

  override def checkParameter(): Unit = {
    // 检查聚合方式
    val availableAggMethod=Set( "avg", "max", "min", "sum", "count")
    aggList.foreach(a=>{
      val aggMethod = a.getAggMethod
      if(!availableAggMethod.contains(aggMethod)){
        throw new IllegalArgumentException(s"$aggMethod is now allow . The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`..")
      }
    })
  }

  override def trans2DataFrame(inputData: InputData): DataFrame = {
    // todo 如果对相同字段，做不同的聚合呢？
    val aggFunMap = aggList.map(c => (c.getColName -> c.getAggMethod)).toMap
    var df = inputData.defaultInput.get
    df = df.groupBy(groupList.map(col): _*).agg(aggFunMap)
    df
  }
}
