package spark.examples

import spark.SparkContext

object ExceptionHandlingTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: ExceptionHandlingTest <master>")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "ExceptionHandlingTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    sc.parallelize(0 until sc.defaultParallelism).foreach { i =>
      if (math.random > 0.75)
        throw new Exception("Testing exception handling")
    }

    System.exit(0)
  }
}
