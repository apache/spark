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

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.metrics.source.{DoubleAccumulatorSource, LongAccumulatorSource}
import org.apache.spark.sql.SparkSession

/**
 * Usage: AccumulatorMetricsTest [numElem]
 *
 * This example shows how to register accumulators against the accumulator source.
 * A simple RDD is created, and during the map, the accumulators are incremented.
 *
 * The only argument, numElem, sets the number elements in the collection to parallize.
 *
 * The result is output to stdout in the driver with the values of the accumulators.
 * For the long accumulator, it should equal numElem the double accumulator should be
 * roughly 1.1 x numElem (within double precision.) This example also sets up a
 * ConsoleSink (metrics) instance, and so registered codahale metrics (like the
 * accumulator source) are reported to stdout as well.
 */
object AccumulatorMetricsTest {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.metrics.conf.*.sink.console.class",
              "org.apache.spark.metrics.sink.ConsoleSink")
      .getOrCreate()

    val sc = spark.sparkContext

    val acc = sc.longAccumulator("my-long-metric")
    // register the accumulator, the metric system will report as
    // [spark.metrics.namespace].[execId|driver].AccumulatorSource.my-long-metric
    LongAccumulatorSource.register(sc, List(("my-long-metric" -> acc)).toMap)

    val acc2 = sc.doubleAccumulator("my-double-metric")
    // register the accumulator, the metric system will report as
    // [spark.metrics.namespace].[execId|driver].AccumulatorSource.my-double-metric
    DoubleAccumulatorSource.register(sc, List(("my-double-metric" -> acc2)).toMap)

    val num = if (args.length > 0) args(0).toInt else 1000000

    val startTime = System.nanoTime

    val accumulatorTest = sc.parallelize(1 to num).foreach(_ => {
      acc.add(1)
      acc2.add(1.1)
    })

    // Print a footer with test time and accumulator values
    println("Test took %.0f milliseconds".format((System.nanoTime - startTime) / 1E6))
    println("Accumulator values:")
    println("*** Long accumulator (my-long-metric): " + acc.value)
    println("*** Double accumulator (my-double-metric): " + acc2.value)

    spark.stop()
  }
}
// scalastyle:on println
