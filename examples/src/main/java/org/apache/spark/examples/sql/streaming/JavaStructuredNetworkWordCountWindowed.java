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

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 * sliding window of configurable duration. Each line from the network is tagged
 * with a timestamp that is used to determine the windows into which it falls.
 *
 * Usage: JavaStructuredNetworkWordCountWindowed <hostname> <port> <window duration>
 *   [<slide duration>]
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 * <window duration> gives the size of window, specified as integer number of seconds
 * <slide duration> gives the amount of time successive windows are offset from one another,
 * given in the same units as above. <slide duration> should be less than or equal to
 * <window duration>. If the two are equal, successive windows have no overlap. If
 * <slide duration> is not provided, it defaults to <window duration>.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.JavaStructuredNetworkWordCountWindowed
 *    localhost 9999 <window duration in seconds> [<slide duration in seconds>]`
 *
 * One recommended <window duration>, <slide duration> pair is 10, 5
 */
public final class JavaStructuredNetworkWordCountWindowed {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: JavaStructuredNetworkWordCountWindowed <hostname> <port>" +
        " <window duration in seconds> [<slide duration in seconds>]");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    int windowSize = Integer.parseInt(args[2]);
    int slideSize = (args.length == 3) ? windowSize : Integer.parseInt(args[3]);
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>");
    }
    String windowDuration = windowSize + " seconds";
    String slideDuration = slideSize + " seconds";

    SparkSession spark = SparkSession
      .builder()
      .appName("JavaStructuredNetworkWordCountWindowed")
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
    Dataset<Row> words = lines
      .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
      .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
          List<Tuple2<String, Timestamp>> result = new ArrayList<>();
          for (String word : t._1.split(" ")) {
            result.add(new Tuple2<>(word, t._2));
          }
          return result.iterator();
        },
        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
      ).toDF("word", "timestamp");

    // Group the data by window and word and compute the count of each group
    Dataset<Row> windowedCounts = words.groupBy(
      functions.window(words.col("timestamp"), windowDuration, slideDuration),
      words.col("word")
    ).count().orderBy("window");

    // Start running the query that prints the windowed word counts to the console
    StreamingQuery query = windowedCounts.writeStream()
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start();

    query.awaitTermination();
  }
}
