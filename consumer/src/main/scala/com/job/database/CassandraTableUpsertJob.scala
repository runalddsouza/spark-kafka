package com.job.database

import com.configuration.{Configuration, SparkConfiguration}
import com.model.{Electronics, Schema}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraTableUpsertJob(configuration: SparkConfiguration) extends TableUpsertJob(configuration: SparkConfiguration) {

  override protected def init: SparkSession = {
    SparkSession.builder.master(configuration.master)
      .config("spark.cassandra.connection.host", configuration.cassandra.host)
      .config("spark.cassandra.connection.port", configuration.cassandra.port)
      .config("spark.cassandra.auth.username", configuration.cassandra.username)
      .config("spark.cassandra.auth.password", configuration.cassandra.password)
      .config("spark.cassandra.connection.localDC", configuration.cassandra.datacenter)
      .getOrCreate
  }

  override protected def getStream: DataFrame = {
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", configuration.kafka.server)
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required"
        + s"\nusername='${configuration.kafka.userName}'"
        + s"\npassword='${configuration.kafka.password}';")
      .option("subscribe", configuration.kafka.topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
  }

  override protected def schema: Schema = new Electronics()

  override protected def upsert(df: DataFrame, schema: Schema): Unit = {
    df.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema.load).as("electronics"))
      .selectExpr("electronics.*")
      .writeStream
      .outputMode(OutputMode.Update)
      .format("org.apache.spark.sql.cassandra")
      .option("checkpointLocation", configuration.kafka.checkpointLocation)
      .option("keyspace", configuration.cassandra.keyspace)
      .option("table", configuration.cassandra.table)
      .start()
      .awaitTermination()
  }
}

object CassandraTableUpsertJob {
  def main(args: Array[String]): Unit = new CassandraTableUpsertJob(Configuration.load).run()
}
