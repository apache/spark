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

package org.apache.spark.status.protobuf

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.status.protobuf.StoreTypes.{DeterministicLevel => GDeterministicLevel}

object SerializerBenchmark extends BenchmarkBase {

  import org.apache.spark.rdd.DeterministicLevel

  private lazy val scalaToPb = Map(
    DeterministicLevel.DETERMINATE -> GDeterministicLevel.DETERMINISTIC_LEVEL_DETERMINATE,
    DeterministicLevel.UNORDERED -> GDeterministicLevel.DETERMINISTIC_LEVEL_UNORDERED,
    DeterministicLevel.INDETERMINATE -> GDeterministicLevel.DETERMINISTIC_LEVEL_INDETERMINATE
  )

  val valuesPerIteration = 1000000

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark = new Benchmark(
      s"Test serialize",
      valuesPerIteration,
      output = output)


    val testValues = DeterministicLevel.values

    benchmark.addCase("Use case match") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        testValues.foreach {
          DeterministicLevelSerializer.serialize
        }
      }
    }

    benchmark.addCase("Use map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        testValues.foreach(scalaToPb)
      }
    }

    benchmark.run()
  }
}

