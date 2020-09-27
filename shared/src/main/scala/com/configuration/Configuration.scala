package com.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Configuration {
  def load: SparkConfiguration = {
    new ObjectMapper(new YAMLFactory)
      .registerModule(DefaultScalaModule).readValue(getClass.getClassLoader.getResourceAsStream("config.yml"), classOf[SparkConfiguration])
  }
}

case class SparkConfiguration(@JsonProperty("master") master: String, @JsonProperty("kafka") kafka: Kafka,
                              @JsonProperty("cassandra") cassandra: Cassandra)

case class Kafka(@JsonProperty("server") server: String, @JsonProperty("username") userName: String,
                 @JsonProperty("password") password: String, @JsonProperty("topic") topic: String,
                 @JsonProperty("checkpointLocation") checkpointLocation: String)

case class Cassandra(@JsonProperty("keyspace") keyspace: String, @JsonProperty("host") host: String, @JsonProperty("port") port: String,
                     @JsonProperty("username") username: String, @JsonProperty("password") password: String, @JsonProperty("datacenter") datacenter: String,
                     @JsonProperty("table") table: String)
