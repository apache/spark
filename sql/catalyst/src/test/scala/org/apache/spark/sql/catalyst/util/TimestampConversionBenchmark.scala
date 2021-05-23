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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Benchmark

/**
 * Benchmark for the timestamp conversion utilities
 */
object TimestampConversionBenchmark {

  def main(args: Array[String]): Unit = {
    val numRows = 1 << 12
    val iters = 1 << 10
    val benchmark = new Benchmark("Timestamp Conversion", iters * numRows.toLong)
    benchmark.addCase("no tz") { _: Int =>
      var sum = 0
      val utf8 = UTF8String.fromString("2021-05-24T12:00:00.000")
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numRows) {
          sum += DateTimeUtils.stringToTimestamp(utf8).hashCode()
          i += 1
        }
      }
    }
    benchmark.addCase("with UTC tz") { _: Int =>
      var sum = 0
      val utf8 = UTF8String.fromString("2021-05-24T12:00:00.000Z")
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numRows) {
          sum += DateTimeUtils.stringToTimestamp(utf8).hashCode()
          i += 1
        }
      }
    }
    benchmark.addCase("with hours tz") { _: Int =>
      var sum = 0
      val utf8 = UTF8String.fromString("2021-05-24T12:00:00.000+02:00")
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numRows) {
          sum += DateTimeUtils.stringToTimestamp(utf8).hashCode()
          i += 1
        }
      }
    }
    benchmark.run()
  }
}
