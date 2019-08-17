package com.demo.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val hostname: String = "localhost"
    val port: Int = 9000

    val source = env.socketTextStream(hostname, port)

    val content = source.flatMap(
      _.toLowerCase.split("\\W")
    ).map(
      (_, 1)
    ).keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    content.print()

    env.execute("Window Stream WordCount")
  }
}
