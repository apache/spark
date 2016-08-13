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

package org.apache.spark.mllib.linalg

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.util.Benchmark

/**
 * Serialization benchmark for VectorUDT.
 */
object UDTSerializationBenchmark {

  def main(args: Array[String]): Unit = {
    val iters = 1e2.toInt
    val numRows = 1e3.toInt

    val encoder = ExpressionEncoder[Vector].resolveAndBind()

    val vectors = (1 to numRows).map { i =>
      Vectors.dense(Array.fill(1e5.toInt)(1.0 * i))
    }.toArray
    val rows = vectors.map(encoder.toRow)

    val benchmark = new Benchmark("VectorUDT de/serialization", numRows, iters)

    benchmark.addCase("serialize") { _ =>
      var sum = 0
      var i = 0
      while (i < numRows) {
        sum += encoder.toRow(vectors(i)).numFields
        i += 1
      }
    }

    benchmark.addCase("deserialize") { _ =>
      var sum = 0
      var i = 0
      while (i < numRows) {
        sum += encoder.fromRow(rows(i)).numActives
        i += 1
      }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    VectorUDT de/serialization:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    serialize                                 380 /  392          0.0      379730.0       1.0X
    deserialize                               138 /  142          0.0      137816.6       2.8X
    */
    benchmark.run()
  }
}
