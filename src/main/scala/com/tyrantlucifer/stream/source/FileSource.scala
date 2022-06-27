package com.tyrantlucifer.stream.source

import org.apache.flink.streaming.api.scala._

object FileSource {
  def main(args: Array[String]): Unit = {
    // 生成执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取文件流
    val path = this.getClass.getClassLoader.getResource("persons.txt").getPath
    val stream = environment.readTextFile(path)
    // 对流进行转换
    stream.map(line => {
      val strings = line.split(",")
      Person(strings(0), strings(1).toInt, strings(2).toInt)
    }).print()
    // 启动任务
    environment.execute("flink stream from file")
  }

}
