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

import java.io.File
import java.lang.reflect.Modifier
import java.nio.file.{FileSystems, Paths}
import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.google.common.reflect.ClassPath

/**
 * Run all benchmarks. To run this benchmark, you should build Spark with either Maven or SBT.
 * After that, you can run as below:
 *
 * {{{
 *   1. with spark-submit
 *      bin/spark-submit --class <this class>
 *        --jars <all spark test jars>,<spark external package jars>
 *        <spark core test jar> <glob pattern for class> <extra arguments>
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 bin/spark-submit --class <this class>
 *        --jars <all spark test jars>,<spark external package jars>
 *        <spark core test jar> <glob pattern for class> <extra arguments>
 *      Results will be written to all corresponding files under "benchmarks/".
 *      Notice that it detects the sub-project's directories from jar's paths so the provided jars
 *      should be properly placed under target (Maven build) or target/scala-* (SBT) when you
 *      generate the files.
 * }}}
 *
 * You can use a command as below to find all the test jars.
 * Make sure to do not select duplicated jars created by different versions of builds or tools.
 * {{{
 *   find . -name '*-SNAPSHOT-tests.jar' | paste -sd ',' -
 * }}}
 *
 * The example below runs all benchmarks and generates the results:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 bin/spark-submit --class \
 *     org.apache.spark.benchmark.Benchmarks --jars \
 *     "`find . -name '*-SNAPSHOT-tests.jar' -o -name '*avro*-SNAPSHOT.jar' | paste -sd ',' -`" \
 *     "`find . -name 'spark-core*-SNAPSHOT-tests.jar'`" \
 *     "*"
 * }}}
 *
 * The example below runs all benchmarks under "org.apache.spark.sql.execution.datasources"
 * {{{
 *   bin/spark-submit --class \
 *     org.apache.spark.benchmark.Benchmarks --jars \
 *     "`find . -name '*-SNAPSHOT-tests.jar' -o -name '*avro*-SNAPSHOT.jar' | paste -sd ',' -`" \
 *     "`find . -name 'spark-core*-SNAPSHOT-tests.jar'`" \
 *     "org.apache.spark.sql.execution.datasources.*"
 * }}}
 */

object Benchmarks {
  var currentProjectRoot: Option[String] = None

  def main(args: Array[String]): Unit = {
    val isFailFast = sys.env.get(
      "SPARK_BENCHMARK_FAILFAST").map(_.toLowerCase(Locale.ROOT).trim.toBoolean).getOrElse(true)
    val numOfSplits = sys.env.get(
      "SPARK_BENCHMARK_NUM_SPLITS").map(_.toLowerCase(Locale.ROOT).trim.toInt).getOrElse(1)
    val currentSplit = sys.env.get(
      "SPARK_BENCHMARK_CUR_SPLIT").map(_.toLowerCase(Locale.ROOT).trim.toInt - 1).getOrElse(0)
    var numBenchmark = 0

    var isBenchmarkFound = false
    val benchmarkClasses = ClassPath.from(
      Thread.currentThread.getContextClassLoader
    ).getTopLevelClassesRecursive("org.apache.spark").asScala.toArray
    val matcher = FileSystems.getDefault.getPathMatcher(s"glob:${args.head}")

    benchmarkClasses.foreach { info =>
      lazy val clazz = info.load
      lazy val runBenchmark = clazz.getMethod("main", classOf[Array[String]])
      // isAssignableFrom seems not working with the reflected class from Guava's
      // getTopLevelClassesRecursive.
      require(args.length > 0, "Benchmark class to run should be specified.")
      if (
          info.getName.endsWith("Benchmark") &&
          matcher.matches(Paths.get(info.getName)) &&
          Try(runBenchmark).isSuccess && // Does this has a main method?
          !Modifier.isAbstract(clazz.getModifiers) // Is this a regular class?
      ) {
        numBenchmark += 1
        if (numBenchmark % numOfSplits == currentSplit) {
          isBenchmarkFound = true

          val targetDirOrProjDir =
            new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI)
              .getParentFile.getParentFile

          // The root path to be referred in each benchmark.
          currentProjectRoot = Some {
            if (targetDirOrProjDir.getName == "target") {
              // SBT build
              targetDirOrProjDir.getParentFile.getCanonicalPath
            } else {
              // Maven build
              targetDirOrProjDir.getCanonicalPath
            }
          }

          // scalastyle:off println
          println(s"Running ${clazz.getName}:")
          // scalastyle:on println
          // Force GC to minimize the side effect.
          System.gc()
          try {
            runBenchmark.invoke(null, args.tail.toArray)
          } catch {
            case e: Throwable if !isFailFast =>
              // scalastyle:off println
              println(s"${clazz.getName} failed with the exception below:")
              // scalastyle:on println
              e.printStackTrace()
          }
        }
      }
    }

    if (!isBenchmarkFound) throw new RuntimeException("No benchmark found to run.")
  }
}
