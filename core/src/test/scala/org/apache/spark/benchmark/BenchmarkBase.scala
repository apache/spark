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

package org.apache.spark.benchmark

import java.io.{File, FileOutputStream, OutputStream}

import org.apache.spark.internal.config.Tests.IS_TESTING

/**
 * A base class for generate benchmark results to a file.
 * For JDK 21+, JDK major version number is added to the file names to distinguish the results.
 */
abstract class BenchmarkBase {
  var output: Option[OutputStream] = None

  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  def runBenchmarkSuite(mainArgs: Array[String]): Unit

  final def runBenchmark(benchmarkName: String)(func: => Any): Unit = {
    val separator = "=" * 96
    val testHeader = (separator + '\n' + benchmarkName + '\n' + separator + '\n' + '\n').getBytes
    output.foreach(_.write(testHeader))
    func
    output.foreach(_.write('\n'))
  }

  def main(args: Array[String]): Unit = {
    // turning this on so the behavior between running benchmark via `spark-submit` or SBT will
    // be consistent, also allow users to turn on/off certain behavior such as
    // `spark.sql.codegen.factoryMode`
    System.setProperty(IS_TESTING.key, "true")
    val regenerateBenchmarkFiles: Boolean = System.getenv("SPARK_GENERATE_BENCHMARK_FILES") == "1"
    if (regenerateBenchmarkFiles) {
      val version = System.getProperty("java.version").split("\\D+")(0).toInt
      val jdkString = if (version > 17) s"-jdk$version" else ""
      val resultFileName =
        s"${this.getClass.getSimpleName.replace("$", "")}$jdkString$suffix-results.txt"
      val prefix = Benchmarks.currentProjectRoot.map(_ + "/").getOrElse("")
      val dir = new File(s"${prefix}benchmarks/")
      if (!dir.exists()) {
        // scalastyle:off println
        println(s"Creating ${dir.getAbsolutePath} for benchmark results.")
        // scalastyle:on println
        dir.mkdirs()
      }
      val file = new File(dir, resultFileName)
      if (!file.exists()) {
        file.createNewFile()
      }
      output = Some(new FileOutputStream(file))
    }

    runBenchmarkSuite(args)

    output.foreach { o =>
      if (o != null) {
        o.close()
      }
    }

    afterAll()
  }

  def suffix: String = ""

  /**
   * Any shutdown code to ensure a clean shutdown
   */
  def afterAll(): Unit = {}
}
