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
package org.apache.spark.examples.sql.streaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 * <p>
 * Usage: JavaStructuredSessionization <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.JavaStructuredSessionization
 * localhost 9999`
 */
public final class JavaStructuredSessionization {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaStructuredSessionization <hostname> <port>");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    SparkSession spark = SparkSession
        .builder()
        .appName("JavaStructuredSessionization")
        .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to host:port
    Dataset<Row> lines = spark
        .readStream()
        .format("socket")
        .option("host", host)
        .option("port", port)
        .option("includeTimestamp", true)
        .load();

    // Split the lines into words, retaining timestamps
    // split() splits each line into an array, and explode() turns the array into multiple rows
    // treat words as sessionId of events
    Dataset<Row> events = lines
        .selectExpr("explode(split(value, ' ')) AS sessionId", "timestamp AS eventTime");

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session updates.
    Dataset<Row> sessionUpdates = events
        .groupBy(session_window(col("eventTime"), "10 seconds").as("session"), col("sessionId"))
        .agg(count("*").as("numEvents"))
        .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
            "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
            "numEvents");

    // Start running the query that prints the session updates to the console
    StreamingQuery query = sessionUpdates
        .writeStream()
        .outputMode("update")
        .format("console")
        .start();

    query.awaitTermination();
  }
}
