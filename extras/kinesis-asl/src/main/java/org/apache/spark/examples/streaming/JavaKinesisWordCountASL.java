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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
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
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.collect.Lists;

/**
 * Java-friendly Kinesis Spark Streaming WordCount example
 *
 * See http://spark.apache.org/docs/latest/streaming-kinesis.html for more details
 * on the Kinesis Spark Streaming integration.
 *
 * This example spins up 1 Kinesis Worker (Spark Streaming Receiver) per shard
 *   for the given stream.
 * It then starts pulling from the last checkpointed sequence number of the given
 *   <stream-name> and <endpoint-url>. 
 *
 * Valid endpoint urls:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
 *
 * This example requires the AWS credentials to be passed as args.
 *
 * Usage: JavaKinesisWordCountASL <app-name> <stream-name> <endpoint-url> <region-name> \
 *                                <aws-access-key-id> <aws-secret-key>
 *   <app-name> name of the consumer app
 *   <stream-name> name of the Kinesis stream (ie. mySparkStream)
 *   <endpoint-url> endpoint of the Kinesis service
 *     (ie. https://kinesis.us-east-1.amazonaws.com)
 *   <region-name> region name for DynamoDB and CloudWatch backing services
 *   <aws-access-key-id> AWS access key id
 *   <aws-secret-key> AWS secret key
 *
 * Examples:
 *    $ SPARK_HOME/bin/run-example \
 *        org.apache.spark.examples.streaming.JavaKinesisWordCountASL myKinesisApp myKinesisStream\
 *        https://kinesis.us-east-1.amazonaws.com us-east-1 <aws-access-key-id> <aws-secret-key>
 *
 * There is a companion helper class called KinesisWordCountProducerASL which puts dummy data 
 *   onto the Kinesis stream.
 *
 * Usage instructions for KinesisWordCountProducerASL are provided in the class definition.
 */
public final class JavaKinesisWordCountASL { // needs to be public for access from run-example
    private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");
    private static final Logger logger = Logger.getLogger(JavaKinesisWordCountASL.class);

    /* Make the constructor private to enforce singleton */
    private JavaKinesisWordCountASL() {
    }

    public static void main(String[] args) {
        /* Check that all required args were passed in. */
        if (args.length < 6) {
          System.err.println(
              "Usage: JavaKinesisWordCountASL <app-name> <stream-name> <endpoint-url>" + 
              "                            <region-name> <aws-access-key-id> <aws-secret-key>\n" +
              "    <app-name> is the name of the consumer app\n" +
              "    <stream-name> is the name of the Kinesis stream\n" +
              "    <endpoint-url> is the endpoint of the Kinesis service\n" +
              "                   (e.g. https://kinesis.us-east-1.amazonaws.com)\n" +
              "    <region-name> region name for DynamoDB and CloudWatch backing services\n" +
              "    <aws-access-key-id> is the AWS Access Key Id\n" + 
              "    <aws-secret-key> is the AWS Secret Key");
          System.exit(1);
        }

        StreamingExamples.setStreamingLogLevels();

        /* Populate the appropriate variables from the given args */
        String appName = args[0];
        String streamName = args[1];
        String endpointUrl = args[2];
        String regionName = args[3];
        String awsAccessKeyId = args[4];
        String awsSecretKey = args[5];
        
        /* Set the batch interval to a fixed 2000 millis (2 seconds) */
        Duration batchInterval = new Duration(2000);

        /* Create Kinesis client in order to determine the number of shards for the given stream */
        AmazonKinesisClient kinesisClient = new AmazonKinesisClient(
                new BasicAWSCredentials(awsAccessKeyId, awsSecretKey));
        kinesisClient.setEndpoint(endpointUrl);

        /* Determine the number of shards from the stream */
        int numShards = kinesisClient.describeStream(streamName)
                .getStreamDescription().getShards().size();

        /* Create 1 Kinesis Worker/Receiver/DStream for each shard */ 
        int numStreams = numShards;

        /* Kinesis checkpoint interval.  Same as batchInterval for this example. */
        Duration checkpointInterval = batchInterval;

        /* Setup the Spark config. */
        SparkConf sparkConfig = new SparkConf().setAppName(appName);

        /* Setup the StreamingContext */
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

        /* Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards */
        List<JavaDStream<byte[]>> streamsList = new ArrayList<JavaDStream<byte[]>>(numStreams);
        for (int i = 0; i < numStreams; i++) {
          streamsList.add(
            KinesisUtils.createStream(appName, jssc, streamName, endpointUrl, 
                regionName, awsAccessKeyId, awsSecretKey, checkpointInterval, 
                InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2())
          );
        }

        /* Union all the streams if there is more than 1 stream */
        JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            unionStreams = jssc.union(streamsList.get(0), 
                streamsList.subList(1, streamsList.size()));
        } else {
            /* Otherwise, just use the 1 stream */
            unionStreams = streamsList.get(0);
        }

        /*
         * Split each line of the union'd DStreams into multiple words using flatMap
         *   to produce the collection.
         * Convert lines of byte[] to multiple Strings by first converting to String, 
         *   then splitting on WORD_SEPARATOR.
         */
        JavaDStream<String> words = unionStreams.flatMap(new FlatMapFunction<byte[], String>() {
                @Override
                public Iterable<String> call(byte[] line) {
                    return Lists.newArrayList(WORD_SEPARATOR.split(new String(line)));
                }
            });

        /* Map each word to a (word, 1) tuple, then reduce/aggregate by word. */
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
            new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) {
                    return new Tuple2<String, Integer>(s, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer i1, Integer i2) {
                  return i1 + i2;
                }
            });

        /* Print the first 10 wordCounts */
        wordCounts.print();

        /* Start the streaming context and await termination */
        jssc.start();
        jssc.awaitTermination();
    }
}
