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
package org.apache.spark.examples.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType}

/**
 * Computes the average signal from IoT device readings over a sliding window of
 * configurable duration. The readings are received over the network and must be
 * UTF8-encoded and separated by '\n'.
 *
 * A single reading should take the format
 * <device name (string)>, <reading (double)>, <time (timestamp)>
 *
 * Usage: EventTimeWindow <hostname> <port> <window duration>
 *   <slide duration> <checkpoint dir>
 * <hostname> and <port> describe the TCP server that Structured Streaming would connect to
 * receive data.
 * <window duration> gives the size of window, specified as integer number of seconds, minutes,
 * or days, e.g. "1 minute", "2 seconds"
 * <slide duration> gives the amount of time successive windows are offset from one another,
 * given in the same units as above
 * (<window duration> and <slide duration> must be enclosed by quotes to ensure that
 * they are processed as individual arguments)
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.EventTimeWindow
 *    localhost 9999 <window duration> <slide duration> <checkpoint dir>`
 *
 * Type device readings in the format given above into Netcat.
 *
 * An example sequence of device readings:
 * dev0,7.0,2015-03-18T12:00:00
 * dev1,8.0,2015-03-18T12:00:10
 * dev0,5.0,2015-03-18T12:00:20
 * dev1,3.0,2015-03-18T12:00:30
 */
object EventTimeWindow {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: EventTimeWindow <hostname> <port> <window duration>" +
        "<slide duration> <checkpoint dir>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2)
    val slideSize = args(3)
    val checkpointDir = args(4)

    val spark = SparkSession
      .builder
      .appName("EventTimeWindow")
      .getOrCreate()

    // Create DataFrame representing the stream of input readings from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // Split the readings into their individual components
    val splitLines = lines.select(
      split(lines.col("value"), ",").alias("pieces")
    )

    // Place the different components of the readings into different columns and
    // cast them appropriately
    val formatted = splitLines.select(
      trim(splitLines.col("pieces").getItem(0)).as("device"),
      trim(splitLines.col("pieces").getItem(1)).cast(DoubleType).as("signal"),
      trim(splitLines.col("pieces").getItem(2)).cast(TimestampType).as("time")
    )

    // Group the readings into windows and compute the signal average within each window
    val windowedAvgs = formatted.groupBy(
      window(formatted.col("time"), windowSize, slideSize)
    ).avg("signal")

    // Start running the query that prints the windowed averages to the console
    val query = windowedAvgs.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", checkpointDir)
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
