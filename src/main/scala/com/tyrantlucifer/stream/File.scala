package com.tyrantlucifer.stream

import org.apache.flink.streaming.api.scala._

object File {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = environment.readTextFile("/home/tyrantlucifer/IdeaProjects/flink-practice/src/main/resources/persons.txt")

    stream.map(line => {
      val strings = line.split(",")
      Person(strings(0), strings(1).toInt, strings(2).toInt)
    }).print()

    environment.execute("flink stream from file")
  }

}
