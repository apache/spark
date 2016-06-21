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
import org.apache.spark.sql.streaming.OutputMode;

import java.util.regex.Pattern;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: JavaStructuredNetworkWordCount <hostname> <port> <checkpoint dir>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.sql.streaming.JavaStructuredNetworkWordCount
 *    localhost 9999 <checkpoint dir>`
 */
public class JavaStructuredNetworkWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: JavaNetworkWordCount <hostname> <port> <checkpoint dir>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("JavaStructuredNetworkWordCount")
      .getOrCreate();

    Dataset<String> df = spark.readStream().format("socket").option("host", args[0])
      .option("port", args[1]).load().as(Encoders.STRING());

    Dataset<String> words = df.select(functions.explode(functions.split(df.col("value"), " ")).alias("word"))
      .as(Encoders.STRING());

    Dataset<Row> wordCounts = words.groupBy("word").count();

    wordCounts.writeStream()
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("checkpointLocation", args[2])
      .start()
      .awaitTermination();

    spark.stop();
  }
}
