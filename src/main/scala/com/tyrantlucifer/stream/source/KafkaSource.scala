package com.tyrantlucifer.stream.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object KafkaSource {
  def main(args: Array[String]): Unit = {
    // 获取流处理运行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 生成kafka需要的配置，实际上不需要设置kafka数据的序列化类，因为flink会自动设置
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.0.6:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer")

    // 生成kafka source
    val kafkaSource = new FlinkKafkaConsumer[String]("test-topic", new SimpleStringSchema(), properties)

    // 在运行环境中添加source
    val stream = environment.addSource(kafkaSource)

    // 打印流
    stream.print()

    // 启动执行环境
    environment.execute("flink stream kafka")

  }
}
