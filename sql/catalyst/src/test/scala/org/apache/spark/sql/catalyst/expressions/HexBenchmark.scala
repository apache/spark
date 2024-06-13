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

package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import org.apache.commons.codec.binary.{Hex => ApacheHex}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.unsafe.types.UTF8String

object HexBenchmark extends BenchmarkBase {

  private val hexStrings = {
    var tmp = Seq("", "A", "AB", "ABC", "ABCD", "123ABCDEF")
    tmp = tmp ++ tmp.map(_.toLowerCase(Locale.ROOT))
    (2 to 4).foreach { i => tmp = tmp ++ tmp.map(x => x * i) }
    tmp.map(UTF8String.fromString(_).toString)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Hex Comparison") {
      Seq(1_000_000, 2_000_000, 4_000_000, 8_000_000).foreach { cardinality =>
        val N = cardinality
        val benchmark = new Benchmark(s"Cardinality $N", N, 3, output = output)
        benchmark.addCase("Apache") { _ =>
          (1 to N).foreach(_ => hexStrings.foreach(y => apacheDecodeHex(y)))
        }

        benchmark.addCase("Spark") { _ =>
          (1 to N).foreach(_ => hexStrings.foreach(y => builtinUnHex(y)))
        }
        benchmark.addCase("Java") { _ =>
          (1 to N).foreach(_ => hexStrings.foreach(y => javaUnhex(y)))
        }
        benchmark.run()
      }
    }
  }

  def apacheDecodeHex(value: String): Literal = {
    val padding = if (value.length % 2 != 0) "0" else ""
    Literal(ApacheHex.decodeHex(padding + value))
  }

  def builtinUnHex(value: String): Literal = {
    val bytes = Hex.unhex(value)
    Literal(bytes, BinaryType)
  }

  def javaUnhex(value: String): Literal = {
    val padding = if (value.length % 2 != 0) "0" else ""
    Literal(java.util.HexFormat.of().parseHex(padding + value), BinaryType)
  }
}
