package com.tyrantlucifer.util

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

import java.io.FileReader
import java.util.Properties

/**
 * @author chao.tian
 * @date 2022/6/14
 */
object KafkaUtil {

  /**
   * load生产者和消费者共同的参数，比如额外认证信息
   * @param properties 配置
   */
  private def loadCommonProperties(properties: Properties)(implicit parameters: ParameterTool): Unit = {
    if (StringUtils.isNotBlank(parameters.get("kafka.properties"))) {
      properties.load(new FileReader(parameters.get("kafka.properties")))
    }
  }

  /**
   * 获取生产者标准配置模板
   * @param servers kafka集群地址
   * @return
   */
  def getProducerProperties(servers: String)(implicit parameters: ParameterTool): Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "3600000")
    loadCommonProperties(properties)
    properties
  }

  /**
   * 获取消费者标准配置模板
   * @param servers kafka集群地址
   * @return
   */
  def getConsumerProperties(servers: String)(implicit parameters: ParameterTool): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, parameters.get("kafka.groupId"))
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    loadCommonProperties(properties)
    properties
  }

  /**
   * 根据kafka集群地址获取标准api版本kafka producer
   * @param servers kafka集群地址
   * @return
   */
  def getProducer(servers: String)(implicit parameters: ParameterTool): KafkaProducer[String, String] = {
    val properties = getProducerProperties(servers)
    new KafkaProducer[String, String](properties)
  }

  /**
   * 根据kafka集群地址获取标准api版本kafka consumer
   * @param servers kafka集群地址
   * @return
   */
  def getConsumer(servers: String)(implicit parameters: ParameterTool): KafkaConsumer[String, String] = {
    val properties = getConsumerProperties(servers)
    new KafkaConsumer[String, String](properties)
  }

  /**
   * 获取flink版本的kafka消费者
   * @param servers kafka集群地址
   * @param topic 要消费的topic
   * @return
   */
  def getFlinkKafkaSource(servers: String, topic: String)(implicit parameters: ParameterTool): FlinkKafkaConsumer[String] = {
    val properties = getConsumerProperties(servers)
    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
  }

  /**
   * 获取flink版本的kafka生产者
   * @param servers kafka集群地址
   * @param topic 要写入的topic
   * @return
   */
  def getFlinkKafkaSink(servers: String, topic: String)(implicit parameters: ParameterTool): FlinkKafkaProducer[String] = {
    val properties = getProducerProperties(servers)
    new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), properties)
  }
}
