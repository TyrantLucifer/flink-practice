package com.tyrantlucifer.stream.source

import org.apache.flink.streaming.api.scala._

case class Person(name: String, age: Int, sex: Int)

object Collection {
  def main(args: Array[String]): Unit = {
    // 准备集合数据
    val persons = List(
      Person("zhangsan", 25, 1),
      Person("lisi", 24, 0),
      Person("wangwu", 26, 1)
    )

    // 获取流处理运行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 获取stream
    val stream = environment.fromCollection(persons)

    // 打印流
    stream.print()

    // 启动任务
    environment.execute("flink stream from collection")
  }

}
