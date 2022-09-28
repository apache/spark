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

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.storage.StorageLevel

/**
 * Benchmark to measure Avro data sources write performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar>,
  *              <spark sql test jar>,<spark avro jar>
 *        <spark avro test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "avro/Test/runMain <this class>"
 *      Results will be written to "benchmarks/AvroWriteBenchmark-results.txt".
 *  }}}
 */
object AvroWriteBenchmark extends DataSourceWriteBenchmark {
  private def wideColumnsBenchmark: Unit = {
    import spark.implicits._

    withTempPath { dir =>
      withTempTable("t1") {
        val width = 1000
        val values = 500000
        val files = 20
        val selectExpr = (1 to width).map(i => s"value as c$i")
        // repartition to ensure we will write multiple files
        val df = spark.range(values)
          .map(_ => Random.nextInt).selectExpr(selectExpr: _*).repartition(files)
          .persist(StorageLevel.DISK_ONLY)
        // cache the data to ensure we are not benchmarking range or repartition
        df.noop()
        df.createOrReplaceTempView("t1")
        val benchmark = new Benchmark(s"Write wide rows into $files files", values, output = output)
        benchmark.addCase("Write wide rows") { _ =>
          spark.sql("SELECT * FROM t1").
            write.format("avro").save(s"${dir.getCanonicalPath}/${Random.nextLong.abs}")
        }
        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runDataSourceBenchmark("Avro")
    wideColumnsBenchmark
  }
}
