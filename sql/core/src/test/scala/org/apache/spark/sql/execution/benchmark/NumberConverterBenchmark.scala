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

object NumberConverterBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def convert(numIters: Int, valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark("convert", valuesPerIteration, output = output)

    val positiveValuesDf = spark.sparkContext.parallelize(0 to valuesPerIteration)
      .toDF("value")

    val negativeValuesDf = spark.sparkContext.parallelize(-valuesPerIteration until 0)
      .toDF("value")

    benchmark.addCase("convert positive values from 10 to 16 (unsigned) bases", numIters) { _ =>
      positiveValuesDf.selectExpr("conv(value, 10, 16)").noop()
    }

    benchmark.addCase("convert negative values from 10 to 16 (unsigned) bases", numIters) { _ =>
      negativeValuesDf.selectExpr("conv(value, 10, 16)").noop()
    }

    benchmark.addCase("convert positive values from 10 to -16 (signed) bases", numIters) { _ =>
      positiveValuesDf.selectExpr("conv(value, 10, -16)").noop()
    }

    benchmark.addCase("convert negative values from 10 to -16 (signed) bases", numIters) { _ =>
      negativeValuesDf.selectExpr("conv(value, 10, -16)").noop()
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    val valuesPerIteration = 10000000
    runBenchmark("Benchmark to measure CONV function performance") {
      convert(numIters, valuesPerIteration)
    }
  }
}
