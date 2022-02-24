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
package org.apache.spark.sql.util

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.util.UUID

import com.google.common.io.ByteStreams
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.RandomUtils

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for ByteStreams.toByteArray vs Copy file to ByteArrayOutputStream byte by byte copy
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/FileToByteArrayBenchmark-results.txt".
 * }}}
 */
object FileToByteArrayBenchmark extends BenchmarkBase {

  def testToByteArray(fileSie: Int): Unit = {
    // Prepare data
    val bytes = RandomUtils.nextBytes(fileSie)
    val file = File.createTempFile(s"$fileSie-${UUID.randomUUID()}", ".dat")
    val fileForGuavaApi = File.createTempFile(s"$fileSie-${UUID.randomUUID()}", ".dat")
    file.deleteOnExit()
    fileForGuavaApi.deleteOnExit()
    FileUtils.writeByteArrayToFile(file, bytes)
    FileUtils.writeByteArrayToFile(fileForGuavaApi, bytes)

    val benchmark = new Benchmark(s"ToByteArray with $fileSie ", 1, output = output)

    benchmark.addCase("toByteArray: byte by byte copy") { _: Int =>
      toByteArray(file)
    }

    benchmark.addCase("toByteArray: use Guava api") { _: Int =>
      toByteArrayUseGuava(fileForGuavaApi)
    }
    benchmark.run()
  }

  private def toByteArrayUseGuava(file: File): Array[Byte] = {
    val inStream = new FileInputStream(file)
    try {
      ByteStreams.toByteArray(inStream)
    } finally {
      inStream.close()
    }
  }

  private def toByteArray(file: File): Array[Byte] = {
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while (reading) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    } finally {
      inStream.close()
    }
    outStream.toByteArray
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Test 1K, 1M, 10M, 100M fileSize
    Seq(1024, 1024 * 1024, 1024 * 1024 * 10, 1024 * 1024 * 100).foreach { fileSize =>
      testToByteArray(fileSize)
    }
  }
}
