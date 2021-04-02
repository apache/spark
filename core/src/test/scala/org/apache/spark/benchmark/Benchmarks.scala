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

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.common.reflect.ClassPath

/**
 * Run all benchmarks. To run this benchmark, you should build Spark with either Maven or SBT.
 * After that, you can run as below:
 *
 * {{{
 *   1. with spark-submit
 *      bin/spark-submit --class <this class> --jars <all spark test jars> <spark core test jar>
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 bin/spark-submit --class <this class> --jars
 *        <all spark test jars> <spark core test jar>
 *      Results will be written to all corresponding files under "benchmarks/".
 *      Notice that it detects the sub-project's directories from jar's paths so the provided jars
 *      should be properly placed under target (Maven build) or target/scala-* (SBT) when you
 *      generate the files.
 * }}}
 *
 * In Mac, you can use a command as below to find all the test jars.
 * {{{
 *   find . -name "*3.2.0-SNAPSHOT-tests.jar" | paste -sd ',' -
 * }}}
 *
 * Full command example:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 bin/spark-submit --class \
 *     org.apache.spark.benchmark.Benchmarks --jars \
 *     "`find . -name "*3.2.0-SNAPSHOT-tests.jar" | paste -sd ',' -`" \
 *     ./core/target/scala-2.12/spark-core_2.12-3.2.0-SNAPSHOT-tests.jar
 * }}}
 */

object Benchmarks {
  def main(args: Array[String]): Unit = {
    ClassPath.from(
      Thread.currentThread.getContextClassLoader
    ).getTopLevelClassesRecursive("org.apache.spark").asScala.foreach { info =>
      lazy val clazz = info.load
      lazy val runBenchmark = clazz.getMethod("main", classOf[Array[String]])
      // isAssignableFrom seems not working with the reflected class from Guava's
      // getTopLevelClassesRecursive.
      if (
          info.getName.endsWith("Benchmark") &&
          Try(runBenchmark).isSuccess && // Does this has a main method?
          !Modifier.isAbstract(clazz.getModifiers) // Is this a regular class?
      ) {
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
        runBenchmark.invoke(null, Array(projDir))
      }
    }
  }
}
