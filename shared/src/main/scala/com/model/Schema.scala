package com.model

import org.apache.spark.sql.types._

trait Schema {
  def load: StructType
}

class Electronics extends Schema {
  override def load: StructType = {
    StructType(Seq(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("code", StringType),
      StructField("type", StringType),
      StructField("brand", StringType),
      StructField("links", ArrayType(StructType(Seq(StructField("type", StringType), StructField("link", StringType))))),
      StructField("details", MapType(StringType, StringType, valueContainsNull = true))
    ))
  }
}

