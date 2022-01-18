package com.tyrantlucifer.stream

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

object Kafka {
  def main(args: Array[String]): Unit = {
    // 获取流处理运行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.18.0.6:9092")
    properties.setProperty("group.id", "flink-consumer")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaSource = new FlinkKafkaConsumer011[String]("test-topic", new SimpleStringSchema(), properties)

    val stream = environment.addSource(kafkaSource)

    stream.print()

    environment.execute("flink stream kafka")

  }
}
