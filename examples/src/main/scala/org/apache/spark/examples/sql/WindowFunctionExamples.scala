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
package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * An example that explains the usage of window functions.
 *
 * It shows the difference between no/unbounded/bounded window frames and how they are resolved.
 * The example also embraces 2 ways to define window frames: based on physical (rowsBetween)
 * and logical (rangeBetween) offsets.
 *
 * All window functions can be divided into 2 groups: functions defined by the
 * [[org.apache.spark.sql.catalyst.expressions.WindowFunction]] trait and all other supported
 * functions that are marked as window functions by providing a window specification. The main
 * distinction is that the first group might have a predefined internal frame.
 *
 * The following cases are possible while resolving window frames for a window function:
 *
 * 1. If the function belongs to the first group (e.g. rank, lead, lag), its frame is specified
 * internally and there is a user defined frame, then those frames must match
 * 2. If the function belongs to the first group (e.g. rank, lead, lag), its frame is specified
 * internally and there is NO user defined frame, then the internal one is used
 * 3. If the function belongs to the second group and there is a user defined frame, then it is used
 * 4. If the function belongs to the second group and there is NO user defined frame, then
 * a default one is assigned. If there is an order specification and the window function supports
 * user specified frames, then the frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
 * Otherwise, it is ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 *
 * @see [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveWindowFrame]]
 */
object WindowFunctionExamples {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
        .appName("Window Function Examples")
        .getOrCreate()

    import spark.implicits._

    val routeEventDataFrame = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("examples/src/main/resources/routeEvents.csv")

    routeEventDataFrame.printSchema()

    // Only partitioning and ordering, no frame boundaries
    val noFrameWinSpec = Window.partitionBy('transportType, 'routeNumber)
        .orderBy('eventTime.asc)
    // Partitioning, ordering, logical offset frame boundaries
    val unboundedFrameWinSpec = Window.partitionBy('transportType, 'routeNumber)
        .orderBy('eventTime.asc)
        .rangeBetween(Long.MinValue, Long.MaxValue)
    // Partitioning, ordering, physical offset frame boundaries
    val boundedFrameWinSpec = Window.partitionBy('transportType, 'routeNumber)
        .orderBy('eventTime.asc)
        .rowsBetween(1, 1)

    // firstGroupEvent -> 4th case. There is an ordering, so the final frame is RANGE BETWEEN
    // UNBOUNDED PRECEDING AND CURRENT ROW

    // lastGroupEvent -> 3rd case. The final frame is RANGE BETWEEN UNBOUNDED PRECEDING AND
    // UNBOUNDED FOLLOWING

    // nextGroupEvent -> 1st case. The final frame is ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING

    // previousGroupEvent -> 2nd case. The final frame is ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING

    // averageGroupDeviation -> 3rd case. The final frame is RANGE BETWEEN UNBOUNDED PRECEDING AND
    // UNBOUNDED FOLLOWING

    // rankInGroup -> 2nd case. The final frame is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

    routeEventDataFrame.withColumn("fistGroupEvent", first('eventTime).over(noFrameWinSpec))
        .withColumn("lastGroupEvent", last('eventTime).over(unboundedFrameWinSpec))
        .withColumn("nextGroupEvent", lead('eventTime, 1).over(boundedFrameWinSpec))
        .withColumn("previousGroupEvent", lag('eventTime, 1).over(noFrameWinSpec))
        .withColumn("averageGroupDeviation", avg('deviation).over(unboundedFrameWinSpec))
        .withColumn("rankInGroup", rank().over(noFrameWinSpec))
        .show(truncate = false)

    spark.stop()
  }
}
