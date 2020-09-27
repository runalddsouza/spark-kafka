package com.job.database

import com.configuration.SparkConfiguration
import com.job.SparkJob
import com.model.Schema
import org.apache.spark.sql.DataFrame

abstract class TableUpsertJob(configuration: SparkConfiguration) extends SparkJob {
  final protected def execute(): Unit = {
    upsert(getStream, schema)
  }

  protected def schema: Schema

  protected def getStream: DataFrame

  protected def upsert(df: DataFrame, schema: Schema)
}
