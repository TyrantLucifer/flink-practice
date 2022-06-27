package com.tyrantlucifer.stream.source

import org.apache.flink.streaming.api.scala._

object SocketSource {
  def main(args: Array[String]): Unit = {
    // 获取流处理运行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 获取socket数据
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
