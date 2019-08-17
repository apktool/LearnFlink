package com.demo.batch

import org.apache.flink.api.scala._

/**
  * Skeleton for a Flink Batch Job.
  *
  * For a tutorial how to write a Flink batch application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution,
  * change the main class in the POM.xml file to this class (simply search for 'mainClass')
  * and run 'mvn clean package' on the command line.
  */
object WordCount {

  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text: DataSet[String] = env.fromElements(
      """
        | Hello C/C++
        | Hello Python
        | Hello Java
        | Hello Scala
      """.stripMargin)

    val count = text.flatMap(
      _.toLowerCase.split("\\W")
    ).filter(
      _.nonEmpty
    ).map(
      (_, 1)
    ).groupBy(0)
      .sum(1)

    count.print()
  }
}
