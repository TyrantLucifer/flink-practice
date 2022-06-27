package com.tyrantlucifer.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCountExample {
  def main(args: Array[String]): Unit = {
    // 创建批处理运行环境
    val environment = ExecutionEnvironment.getExecutionEnvironment

    // 获取文件路径
    val path = this.getClass.getClassLoader.getResource("data.txt").getPath

    // 执行job
    environment.readTextFile(path)
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 以第一个元素进行分组
      .sum(1) // 对第二个元素进行合并
      .print() // 打印
  }
}
