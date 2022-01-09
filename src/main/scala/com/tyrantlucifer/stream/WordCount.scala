package com.tyrantlucifer.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {
    // 获取流处理运行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 处理源源不断的流数据
    environment.socketTextStream("hadoop001", 7777)
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()

    // 开启作业
    environment.execute()
  }
}
