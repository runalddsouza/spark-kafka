package com.job

import org.apache.spark.sql.SparkSession

trait SparkJob {

  protected var spark: SparkSession = _

  protected def init: SparkSession

  protected def execute(): Unit

  final def run(): Unit = {
    spark = init
    execute()
    finish()
  }

  final protected def finish(): Unit = spark.stop
}
