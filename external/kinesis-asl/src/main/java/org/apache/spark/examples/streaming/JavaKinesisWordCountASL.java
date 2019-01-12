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
package org.apache.spark.examples.streaming;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import scala.Tuple2;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

/**
 * Consumes messages from a Amazon Kinesis streams and does wordcount.
 *
 * This example spins up 1 Kinesis Receiver per shard for the given stream.
 * It then starts pulling from the last checkpointed sequence number of the given stream.
 *
 * Usage: JavaKinesisWordCountASL [app-name] [stream-name] [endpoint-url] [region-name]
 *   [app-name] is the name of the consumer app, used to track the read data in DynamoDB
 *   [stream-name] name of the Kinesis stream (ie. mySparkStream)
 *   [endpoint-url] endpoint of the Kinesis service
 *     (e.g. https://kinesis.us-east-1.amazonaws.com)
 *
 *
 * Example:
 *      # export AWS keys if necessary
 *      $ export AWS_ACCESS_KEY_ID=[your-access-key]
 *      $ export AWS_SECRET_KEY=<your-secret-key>
 *
 *      # run the example
 *      $ SPARK_HOME/bin/run-example   streaming.JavaKinesisWordCountASL myAppName  mySparkStream \
 *             https://kinesis.us-east-1.amazonaws.com
 *
 * There is a companion helper class called KinesisWordProducerASL which puts dummy data
 * onto the Kinesis stream.
 *
 * This code uses the DefaultAWSCredentialsProviderChain to find credentials
 * in the following order:
 *    Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 *    Java System Properties - aws.accessKeyId and aws.secretKey
 *    Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
 *    Instance profile credentials - delivered through the Amazon EC2 metadata service
 * For more information, see
 * http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html
 *
 * See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more details on
 * the Kinesis Spark Streaming integration.
 */
public final class JavaKinesisWordCountASL { // needs to be public for access from run-example
  private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    // Check that all required args were passed in.
    if (args.length != 3) {
      System.err.println(
          "Usage: JavaKinesisWordCountASL <stream-name> <endpoint-url>\n\n" +
          "    <app-name> is the name of the app, used to track the read data in DynamoDB\n" +
          "    <stream-name> is the name of the Kinesis stream\n" +
          "    <endpoint-url> is the endpoint of the Kinesis service\n" +
          "                   (e.g. https://kinesis.us-east-1.amazonaws.com)\n" +
          "Generate data for the Kinesis stream using the example KinesisWordProducerASL.\n" +
          "See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more\n" +
          "details.\n"
      );
      System.exit(1);
    }

    // Set default log4j logging level to WARN to hide Spark logs
    StreamingExamples.setStreamingLogLevels();

    // Populate the appropriate variables from the given args
    String kinesisAppName = args[0];
    String streamName = args[1];
    String endpointUrl = args[2];

    // Create a Kinesis client in order to determine the number of shards for the given stream
    AmazonKinesisClient kinesisClient =
        new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
    kinesisClient.setEndpoint(endpointUrl);
    int numShards =
        kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();


    // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
    // This is not a necessity; if there are less receivers/DStreams than the number of shards,
    // then the shards will be automatically distributed among the receivers and each receiver
    // will receive data from multiple shards.
    int numStreams = numShards;

    // Spark Streaming batch interval
    Duration batchInterval = new Duration(2000);

    // Kinesis checkpoint interval.  Same as batchInterval for this example.
    Duration kinesisCheckpointInterval = batchInterval;

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream
    String regionName = KinesisExampleUtils.getRegionNameByEndpoint(endpointUrl);

    // Setup the Spark config and StreamingContext
    SparkConf sparkConfig = new SparkConf().setAppName("JavaKinesisWordCountASL");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

    // Create the Kinesis DStreams
    List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);
    for (int i = 0; i < numStreams; i++) {
      streamsList.add(
          KinesisUtils.createStream(jssc, kinesisAppName, streamName, endpointUrl, regionName,
              InitialPositionInStream.LATEST, kinesisCheckpointInterval,
              StorageLevel.MEMORY_AND_DISK_2())
      );
    }

    // Union all the streams if there is more than 1 stream
    JavaDStream<byte[]> unionStreams;
    if (streamsList.size() > 1) {
      unionStreams = jssc.union(streamsList.toArray(new JavaDStream[0]));
    } else {
      // Otherwise, just use the 1 stream
      unionStreams = streamsList.get(0);
    }

    // Convert each line of Array[Byte] to String, and split into words
    JavaDStream<String> words = unionStreams.flatMap(new FlatMapFunction<byte[], String>() {
      @Override
      public Iterator<String> call(byte[] line) {
        String s = new String(line, StandardCharsets.UTF_8);
        return Arrays.asList(WORD_SEPARATOR.split(s)).iterator();
      }
    });

    // Map each word to a (word, 1) tuple so we can reduce by key to count the words
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<>(s, 1);
          }
        }
    ).reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        }
    );

    // Print the first 10 wordCounts
    wordCounts.print();

    // Start the streaming context and await termination
    jssc.start();
    jssc.awaitTermination();
  }
}
