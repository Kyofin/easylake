package com.data.easyflow.utils

object TypeUtil {

  /**
   * 将 spark类型转成 simpleString。
   * 参数看 org.apache.spark.sql.types.IntegerType
   */
  def getType(typeName: String): String = {
    typeName match {
      case "int" | "bigint" | "long" => "long"
      case "shot" => "short"
      case "float" | "double" => "double"
      case "string" => "string"
      case "boolean" => "boolean"
      case "timestamp" => "timestamp"
      case _ => "string"

    }
  }

}
