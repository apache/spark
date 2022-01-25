/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.serializer

import scala.concurrent._
// scalastyle:off executioncontextglobal
import scala.concurrent.ExecutionContext.Implicits.global
// scalastyle:on executioncontextglobal
import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.launcher.SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS
import org.apache.spark.serializer.KryoTest._
import org.apache.spark.util.ThreadUtils

/**
 * Benchmark for KryoPool vs old "pool of 1".
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/KryoSerializerBenchmark-results.txt".
 * }}}
 */
object KryoSerializerBenchmark extends BenchmarkBase {

  var sc: SparkContext = null
  val N = 500
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val name = "Benchmark KryoPool vs old\"pool of 1\" implementation"
    runBenchmark(name) {
      val benchmark = new Benchmark(name, N, 10, output = output)
      Seq(true, false).foreach(usePool => run(usePool, benchmark))
      benchmark.run()
    }
  }

  private def run(usePool: Boolean, benchmark: Benchmark): Unit = {
    lazy val sc = createSparkContext(usePool)

    System.clearProperty(IS_TESTING.key)
    benchmark.addCase(s"KryoPool:$usePool") { _ =>
      val futures = for (_ <- 0 until N) yield {
        Future {
          sc.parallelize(0 until 10).map(i => i + 1).count()
        }
      }

      val future = Future.sequence(futures)

      ThreadUtils.awaitResult(future, 10.minutes)
    }
  }

  def createSparkContext(usePool: Boolean): SparkContext = {
    val conf = new SparkConf()
    // SPARK-29282 This is for consistency between JDK8 and JDK11.
    conf.set(EXECUTOR_EXTRA_JAVA_OPTIONS,
      "-XX:+UseParallelGC -XX:-UseDynamicNumberOfGCThreads")
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(KRYO_USER_REGISTRATORS, Seq(classOf[MyRegistrator].getName))
    conf.set(KRYO_USE_POOL, usePool)

    if (sc != null) {
      sc.stop()
    }

    sc = new SparkContext("local-cluster[4,1,1024]", "test", conf)
    sc
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

}
