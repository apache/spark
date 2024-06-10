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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


object MetadataStructBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private val NUM_ROWS = 5000000
  private val NUM_ITERS = 32

  private def withTempData(format: String = "parquet")(f: DataFrame => Unit): Unit = {
    val dir = Utils.createTempDir()
    dir.delete()
    try {
      spark.range(0, NUM_ROWS, 1, 1).toDF("id")
        .withColumn("num1", $"id" + 10)
        .withColumn("num2", $"id" / 10)
        .withColumn("str", concat(lit("a sample string "), $"id".cast("string")))
        .write.format(format).save(dir.getAbsolutePath)
      val df = spark.read.format(format).load(dir.getAbsolutePath)
      f(df)
    } finally {
      Utils.deleteRecursively(dir)
    }
  }

  private def addCase(benchmark: Benchmark, df: DataFrame, metadataCol: Option[String]): Unit = {
    benchmark.addCase(metadataCol.getOrElse("no metadata columns")) { _ =>
      df.select("*", metadataCol.toSeq: _*).noop()
    }
  }

  private def metadataBenchmark(name: String, format: String): Unit = {
    withTempData(format) { df =>
      val metadataCols = df.select(FileFormat.METADATA_NAME).schema
        .fields.head.dataType.asInstanceOf[StructType].fieldNames

      val benchmark = new Benchmark(name, NUM_ROWS, NUM_ITERS, output = output)

      addCase(benchmark, df, None)
      for (metadataCol <- metadataCols) {
        addCase(benchmark, df, Some(s"${FileFormat.METADATA_NAME}.$metadataCol"))
      }
      addCase(benchmark, df, Some(FileFormat.METADATA_NAME))

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Metadata Struct Benchmark") {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
        metadataBenchmark("Vectorized Parquet", "parquet")
      }
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        metadataBenchmark("Parquet-mr", "parquet")
      }
      metadataBenchmark("JSON", "json")
    }
  }
}
