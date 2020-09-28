package com.job

import java.time.Instant
import java.util.UUID

import com.configuration.{Configuration, SparkConfiguration}
import com.model.{Electronics, Schema}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{struct, to_json, udf}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class KafkaProducerJob(configuration: SparkConfiguration) extends ProducerJob(configuration: SparkConfiguration) {
  private val uuid: UserDefinedFunction = udf(() => UUID.randomUUID().toString)
  private val updateDate: UserDefinedFunction = udf(() => Instant.now.toEpochMilli)

  override protected def schema: Schema = new Electronics

  override protected def createDataFrame(dataFrameReader: DataFrameReader): DataFrame = {
    dataFrameReader
      .option("multiLine", value = true)
      .json(configuration.dataSource)
      .drop("id", "lastupdatedon")
      .withColumn("lastupdatedon", updateDate())
      .withColumn("id", uuid())
  }

  override protected def write(df: DataFrame): Unit = {
    df.select(to_json(struct(df.columns.head, df.columns.tail: _*)).as("value"))
      .write.format("kafka")
      .option("kafka.bootstrap.servers", configuration.kafka.server)
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
        + s"\nusername='${configuration.kafka.userName}'"
        + s"\npassword='${configuration.kafka.password}';")
      .option("topic", configuration.kafka.topic)
      .save()
  }

  override protected def init: SparkSession = SparkSession.builder.master(configuration.master).getOrCreate
}

object KafkaProducerJob {
  def main(args: Array[String]): Unit = new KafkaProducerJob(Configuration.load).run()
}
