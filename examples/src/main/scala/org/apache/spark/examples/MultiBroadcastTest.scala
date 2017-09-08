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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * Usage: MultiBroadcastTest [partitions] [numElem]
 */
object MultiBroadcastTest {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Multi-Broadcast Test")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = new Array[Int](num)
    for (i <- 0 until arr1.length) {
      arr1(i) = i
    }

    val arr2 = new Array[Int](num)
    for (i <- 0 until arr2.length) {
      arr2(i) = i
    }

    val barr1 = spark.sparkContext.broadcast(arr1)
    val barr2 = spark.sparkContext.broadcast(arr2)
    val observedSizes: RDD[(Int, Int)] = spark.sparkContext.parallelize(1 to 10, slices).map { _ =>
      (barr1.value.length, barr2.value.length)
    }
    // Collect the small RDD so we can print the observed sizes locally.
    observedSizes.collect().foreach(i => println(i))

    spark.stop()
  }
}
// scalastyle:on println
