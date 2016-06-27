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
import org.apache.spark.sql.types.TimestampType

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: EventTimeWindowExample <hostname> <port> <checkpoint dir>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.sql.streaming.EventTimeWindowExample
 *    localhost 9999 <checkpoint dir>`
 */
object NetworkEventTimeWindow {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: EventTimeWindowExample <hostname> <port> <checkpoint dir>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val checkpointDir = args(2)

    val spark = SparkSession
      .builder
      .appName("EventTimeWindowExample")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load().as[String]

    val formatted = lines.map(l => {
      val els = l.split(",")
      (els(0).trim, els(1).trim.toDouble, els(2).trim)
    })

    val formattedRenamed = formatted.select(formatted.col("_1").alias("device"),
      formatted.col("_2").alias("signal"), formatted.col("_3").cast(TimestampType).as("time"))

    val windowedAvgs = formattedRenamed.groupBy(
      window(formattedRenamed.col("time"), "1 minute")).avg("signal")

    // Start running the query that prints the running averages to the console
    val query = windowedAvgs.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", checkpointDir)
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
