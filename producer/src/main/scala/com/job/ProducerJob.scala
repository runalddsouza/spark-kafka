package com.job

import com.configuration.SparkConfiguration
import com.model.Schema
import org.apache.spark.sql.{DataFrame, DataFrameReader}

abstract class ProducerJob(configuration: SparkConfiguration) extends SparkJob {
  final protected def execute(): Unit = write(createDataFrame(getDataFrameReader(schema)))

  protected def schema: Schema

  final def getDataFrameReader(schema: Schema): DataFrameReader = spark.read.schema(schema.load)

  protected def createDataFrame(dataFrameReader: DataFrameReader): DataFrame

  protected def write(df: DataFrame)
}