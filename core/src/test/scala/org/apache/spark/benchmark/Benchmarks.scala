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

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.common.reflect.ClassPath

/**
 * Run all benchmarks. To run this benchmark, you should build Spark with either Maven or SBT.
 * After that, you can run as below:
 *
 * {{{
 *   1. with spark-submit
 *      bin/spark-submit --class <this class> --jars <all spark test jars>
 *        <spark external package jar> <spark core test jar> <glob pattern for class>
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 bin/spark-submit --class <this class> --jars
 *        <all spark test jars> <spark external package jar>
 *        <spark core test jar> <glob pattern for class>
 *      Results will be written to all corresponding files under "benchmarks/".
 *      Notice that it detects the sub-project's directories from jar's paths so the provided jars
 *      should be properly placed under target (Maven build) or target/scala-* (SBT) when you
 *      generate the files.
 * }}}
 *
 * In Mac, you can use a command as below to find all the test jars.
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
  def main(args: Array[String]): Unit = {
    var isBenchmarkFound = false
    ClassPath.from(
      Thread.currentThread.getContextClassLoader
    ).getTopLevelClassesRecursive("org.apache.spark").asScala.foreach { info =>
      lazy val clazz = info.load
      lazy val runBenchmark = clazz.getMethod("main", classOf[Array[String]])
      // isAssignableFrom seems not working with the reflected class from Guava's
      // getTopLevelClassesRecursive.
      val matcher = args.headOption.map(pattern =>
        FileSystems.getDefault.getPathMatcher(s"glob:$pattern"))
      if (
          info.getName.endsWith("Benchmark") &&
          matcher.forall(_.matches(Paths.get(info.getName))) &&
          Try(runBenchmark).isSuccess && // Does this has a main method?
          !Modifier.isAbstract(clazz.getModifiers) // Is this a regular class?
      ) {
        isBenchmarkFound = true
        val targetDirOrProjDir =
          new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI)
          .getParentFile.getParentFile
        val projDir = if (targetDirOrProjDir.getName == "target") {
          // SBT build
          targetDirOrProjDir.getParentFile.getCanonicalPath
        } else {
          // Maven build
          targetDirOrProjDir.getCanonicalPath
        }
        // Force GC to minimize the side effect.
        System.gc()
        runBenchmark.invoke(null, Array(projDir))
      }
    }

    if (!isBenchmarkFound) throw new RuntimeException("No benchmark found to run.")
  }
}
