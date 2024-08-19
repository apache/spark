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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Benchmark for ParquetDictionaryDecoderBenchmark IO
 */
object ParquetDictionaryDecoderBenchmarkDecoderBenchmark extends SqlBasedBenchmark {

  import spark.implicits._

  private val N = 100 * 1024 * 1024
  private val NUMBER_OF_ITER = 10

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { tempDir =>
      val outputPath = tempDir.getCanonicalPath
      val df: DataFrame = spark
        .range(1, N, 1, 1)
        .map(id => ("ABCDEFG", "HIJKLMN", "OPQRST", "UVWXYZ"))
        .toDF("a", "b", "c", "d")
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)
      val benchmark = new Benchmark("Parquet dictionary", N, NUMBER_OF_ITER, output = output)
      benchmark.addCase("Read binary dictionary") { _ =>
        spark.read.parquet(outputPath).noop()
      }
      benchmark.run()
    }
  }

}
