package com.data.easyflow.service

import com.alibaba.fastjson.JSONObject
import com.data.easyflow.utils.TypeUtil
import org.apache.spark.sql.{Row, SparkSession}

object CsvService {

  def parseSchemaAndPreview(spark: SparkSession, path: String,
                            charset: String, sep: String) = {
    // 设置这个线程的action都在这个pool里执行
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "lightweight_tasks")

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("charset", charset)
      .option("sep", sep)
      .load(path)
    val schema = df.schema
    val colNames = schema.names
    val typeNames = schema.map(e => TypeUtil.getType(e.dataType.simpleString)).toArray
    // 字段强制类型转换
    val columns = colNames.zip(typeNames).map(e => df.col(e._1).cast(e._2))
    // 预览前20条
    val rows: Array[Row] = df.select(columns: _*).take(20)
    val dataArray = rows.map(r => {
      r.toSeq.toArray
    })

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)

    // 根据首行获取字段类型
    val jSONObject = new JSONObject()
    jSONObject.put("data", dataArray)
    jSONObject.put("colType", typeNames)
    jSONObject.put("colName", colNames)
    jSONObject
  }
}
