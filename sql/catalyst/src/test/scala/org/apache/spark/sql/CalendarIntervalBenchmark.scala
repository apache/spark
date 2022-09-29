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

package org.apache.spark.sql

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Benchmark for read/write CalendarInterval with two int vs
 * read/write CalendarInterval with one long.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CalendarIntervalBenchmark-results.txt".
 * }}}
 */
object CalendarIntervalBenchmark extends BenchmarkBase {

  def test(name: String, schema: StructType, numRows: Int, iters: Int): Unit = {
    assert(schema.length == 1)
    assert(schema.head.dataType.isInstanceOf[CalendarIntervalType])
    runBenchmark(name) {
      val generator = RandomDataGenerator.forType(schema, nullable = false).get
      val toRow = RowEncoder(schema).createSerializer()
      val attrs = schema.toAttributes
      val safeProjection = GenerateSafeProjection.generate(attrs, attrs)

      val rows = (1 to numRows).map(_ =>
        // The output of encoder is UnsafeRow, use safeProjection to turn in into safe format.
        safeProjection(toRow(generator().asInstanceOf[Row])).copy()
      ).toArray

      val row = InternalRow.apply(new CalendarInterval(0, 0, 0))
      val unsafeRow = UnsafeProjection.create(Array[DataType](CalendarIntervalType)).apply(row)

      val benchmark =
        new Benchmark("CalendarInterval For " + name, iters * numRows.toLong, output = output)
      benchmark.addCase("Call setInterval & getInterval") { _: Int =>
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            val interval = rows(i).getInterval(0)
            unsafeRow.setInterval(0, interval)
            val newInterval = unsafeRow.getInterval(0)
            assert(interval == newInterval)
            i += 1
          }
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val schema = new StructType().add("interval", CalendarIntervalType)
    test("interval", schema, 1 << 14, 1 << 13)
  }

}
