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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, random, sum, when}

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      System.out.println("Consider providing number of partitions and size of each partition " +
        "as first and second argument.")
    }

    val partitions = if (args.length > 0) args(0).toInt else 2
    val rowsPerPartition = if (args.length > 1) args(1).toLong else 100000L

    System.out.println("Computing Pi with " +
      partitions + " partitions" + (if (args.length < 1) " (default)" else "") + " and " +
      rowsPerPartition + " rows per partition" + (if (args.length < 2) " (default)" else "")
    )

    val spark = SparkSession
      .builder()
      .appName("Spark Pi")
      .getOrCreate()
    import spark.implicits._

    val N = rowsPerPartition * partitions
    val count = spark.range(0, N, 1, partitions)
      .select((random() * 2 - 1).as("x"), (random() * 2 - 1).as("y"))
      .select(sum(when($"x" * $"x" + $"y" * $"y" <= 1, lit(1))))
      .as[Long]
      .head()
    println(s"Pi is roughly ${4.0 * count / N}")
    spark.stop()
  }
}
// scalastyle:on println
