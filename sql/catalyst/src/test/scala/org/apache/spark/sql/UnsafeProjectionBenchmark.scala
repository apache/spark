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
import org.apache.spark.sql.types._

/**
 * Benchmark `UnsafeProjection` for fixed-length/primitive-type fields.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/UnsafeProjectionBenchmark-results.txt".
 * }}}
 */
object UnsafeProjectionBenchmark extends BenchmarkBase {

  def generateRows(schema: StructType, numRows: Int): Array[InternalRow] = {
    val generator = RandomDataGenerator.forType(schema, nullable = false).get
    val toRow = RowEncoder(schema).createSerializer()
    (1 to numRows).map(_ => toRow(generator().asInstanceOf[Row]).copy()).toArray
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("unsafe projection") {
      val iters = 1024 * 16
      val numRows = 1024 * 16

      val benchmark = new Benchmark("unsafe projection", iters * numRows.toLong, output = output)

      val schema1 = new StructType().add("l", LongType, false)
      val attrs1 = schema1.toAttributes
      val rows1 = generateRows(schema1, numRows)
      val projection1 = UnsafeProjection.create(attrs1, attrs1)

      benchmark.addCase("single long") { _ =>
        for (_ <- 1 to iters) {
          var sum = 0L
          var i = 0
          while (i < numRows) {
            sum += projection1(rows1(i)).getLong(0)
            i += 1
          }
        }
      }

      val schema2 = new StructType().add("l", LongType, true)
      val attrs2 = schema2.toAttributes
      val rows2 = generateRows(schema2, numRows)
      val projection2 = UnsafeProjection.create(attrs2, attrs2)

      benchmark.addCase("single nullable long") { _ =>
        for (_ <- 1 to iters) {
          var sum = 0L
          var i = 0
          while (i < numRows) {
            sum += projection2(rows2(i)).getLong(0)
            i += 1
          }
        }
      }

      val schema3 = new StructType()
        .add("boolean", BooleanType, false)
        .add("byte", ByteType, false)
        .add("short", ShortType, false)
        .add("int", IntegerType, false)
        .add("long", LongType, false)
        .add("float", FloatType, false)
        .add("double", DoubleType, false)
      val attrs3 = schema3.toAttributes
      val rows3 = generateRows(schema3, numRows)
      val projection3 = UnsafeProjection.create(attrs3, attrs3)

      benchmark.addCase("7 primitive types") { _ =>
        for (_ <- 1 to iters) {
          var sum = 0L
          var i = 0
          while (i < numRows) {
            sum += projection3(rows3(i)).getLong(0)
            i += 1
          }
        }
      }

      val schema4 = new StructType()
        .add("boolean", BooleanType, true)
        .add("byte", ByteType, true)
        .add("short", ShortType, true)
        .add("int", IntegerType, true)
        .add("long", LongType, true)
        .add("float", FloatType, true)
        .add("double", DoubleType, true)
      val attrs4 = schema4.toAttributes
      val rows4 = generateRows(schema4, numRows)
      val projection4 = UnsafeProjection.create(attrs4, attrs4)

      benchmark.addCase("7 nullable primitive types") { _ =>
        for (_ <- 1 to iters) {
          var sum = 0L
          var i = 0
          while (i < numRows) {
            sum += projection4(rows4(i)).getLong(0)
            i += 1
          }
        }
      }

      benchmark.run()
    }
  }
}
