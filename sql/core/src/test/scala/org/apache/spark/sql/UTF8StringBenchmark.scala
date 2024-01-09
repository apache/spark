
package org.apache.spark.sql

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.util.CollatorFactory
import org.apache.spark.unsafe.types.UTF8String

object UTF8StringBenchmark extends BenchmarkBase {

  /**
   * Synthetic unit benchmark for UTF8String.
   * To run this benchmark:
   * {{{
   *   1. build/sbt "sql/Test/runMain <this class>"
   *   2. generate result:
   *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
   *      Results will be written to "benchmarks/UTF8StringBenchmark-results.txt".
   * }}}
   */

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 100000
    runBenchmark("UTF8 comparison") {
      val benchmark = new Benchmark("Equality checks", N, output = output)

      //noinspection ScalaStyle
      val inputs = Seq(
        (UTF8String.fromString("a" * 100), UTF8String.fromString("a" * 100), "strlen(100) equal strings"),
        (UTF8String.fromString("a" * 100), UTF8String.fromString("b" * 100), "strlen(100) unequal strings"),
        (UTF8String.fromString("a" * 4), UTF8String.fromString("a" * 4), "strlen(4) equal strings"),
      )

      val collationName = "en_US-secondary"
      val collationId = CollatorFactory.installComparator(collationName)

      inputs.foreach(input => {
        val (sourceString, targetString, description) = input

        benchmark.addCase(s"Default collation equality $description") { _ =>
          1.to(10000).foreach(_ => sourceString.equals(targetString))
        }

        benchmark.addCase(s"Default collation comparison $description") { _ =>
          1.to(10000).foreach(_ => sourceString.compare(targetString))
        }

        benchmark.addCase(s"Non default collation through id equality $description") { _ =>
          1.to(10000).foreach(_ => {
            sourceString.installCollationAwareComparator(collationId)
            targetString.installCollationAwareComparator(collationId)
            sourceString.equals(targetString)
          })
        }

        benchmark.addCase(s"Non default collation through id comparison $description") { _ =>
          1.to(10000).foreach(_ => {
            sourceString.installCollationAwareComparator(collationId)
            targetString.installCollationAwareComparator(collationId)
            sourceString.compare(targetString)
          })
        }

        benchmark.addCase(s"Non default collation through name equality $description") { _ =>
          1.to(10000).foreach(_ => {
            sourceString.installCollationAwareComparator(collationName)
            targetString.installCollationAwareComparator(collationName)
            sourceString.equals(targetString)
          })
        }

        benchmark.addCase(s"Non default collation through name comparison $description") { _ =>
          1.to(10000).foreach(_ => {
            sourceString.installCollationAwareComparator(collationName)
            targetString.installCollationAwareComparator(collationName)
            sourceString.compare(targetString)
          })
        }
      })

      benchmark.run()
    }
  }
}