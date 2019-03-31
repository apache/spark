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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf

/**
 * Common base trait to run benchmark with the Dataset and DataFrame API.
 */
trait SqlBasedBenchmark extends BenchmarkBase with SQLHelper {

  protected val spark: SparkSession = getSparkSession

  /** Subclass can override this function to build their own SparkSession */
  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, 1)
      .getOrCreate()
  }

  /** Runs function `f` with whole stage codegen on and off. */
  final def codegenBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase(s"$name wholestage off", numIters = 2) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        f
      }
    }

    benchmark.addCase(s"$name wholestage on", numIters = 5) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        f
      }
    }

    benchmark.run()
  }
}
