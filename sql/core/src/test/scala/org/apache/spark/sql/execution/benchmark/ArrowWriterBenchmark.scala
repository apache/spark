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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark for an ArrowWriter that is reused across batches, as the Arrow-based Python runners,
 * toArrow and Spark Connect results do. Arrow vectors never shrink, so the cases compare the
 * same small batches written after a first batch of the same size and after a large one.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *     --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ArrowWriterBenchmark-results.txt".
 * }}}
 */
object ArrowWriterBenchmark extends BenchmarkBase {

  private val numBatches = 1000
  private val rowsPerBatch = 100
  private val largeBatchRows = 100000

  private def writeBatch(writer: ArrowWriter, row: InternalRow, numRows: Int): Unit = {
    var i = 0
    while (i < numRows) {
      writer.write(row)
      i += 1
    }
    writer.finish()
    writer.reset()
  }

  private def smallBatchesAfter(name: String, dataType: DataType, value: Any): Unit = {
    val schema = new StructType().add("value", dataType)
    val row = InternalRow(value)
    val benchmark = new Benchmark(
      s"$numBatches batches of $rowsPerBatch rows, $name",
      numBatches.toLong * rowsPerBatch,
      output = output)
    Seq(rowsPerBatch, largeBatchRows).foreach { firstBatchRows =>
      benchmark.addTimerCase(s"after a first batch of $firstBatchRows rows") { timer =>
        val writer = ArrowWriter.create(schema, "UTC")
        try {
          writeBatch(writer, row, firstBatchRows)
          timer.startTiming()
          var i = 0
          while (i < numBatches) {
            writeBatch(writer, row, rowsPerBatch)
            i += 1
          }
          timer.stopTiming()
        } finally {
          writer.root.close()
        }
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Reused ArrowWriter") {
      smallBatchesAfter("bigint", LongType, 1L)
      smallBatchesAfter("string", StringType, UTF8String.fromString("a" * 16))
      smallBatchesAfter("array<bigint>", ArrayType(LongType),
        ArrayData.toArrayData(Array.fill(16)(1L)))
      smallBatchesAfter("struct<a: bigint, b: string>",
        new StructType().add("a", LongType).add("b", StringType),
        InternalRow(1L, UTF8String.fromString("a" * 16)))
    }
  }
}
