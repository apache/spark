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
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    VectorUDT de/serialization:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    serialize                                      265 /  318          0.0      265138.5       1.0X
    deserialize                                    155 /  197          0.0      154611.4       1.7X
    */
    benchmark.run()
  }
}
