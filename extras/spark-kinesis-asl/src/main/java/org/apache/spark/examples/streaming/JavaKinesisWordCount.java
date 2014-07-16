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

import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kinesis.KinesisRecordSerializer;
import org.apache.spark.streaming.kinesis.KinesisStringRecordSerializer;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import scala.Tuple2;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * Java-friendly Kinesis Spark Streaming WordCount example
 *
 * See http://spark.apache.org/docs/latest/streaming-programming-guide.html for more details on the Kinesis Spark Streaming integration.
 *
 * This example spins up 1 Kinesis Worker (Spark Streaming Receivers) per shard of the given stream.
 * It then starts pulling from the tip of the given <stream-name> and <endpoint-url> at the given <batch-interval>.
 * Because we're pulling from the tip (InitialPositionInStream.LATEST), only new stream data will be picked up after the KinesisReceiver starts.
 * This could lead to missed records if data is added to the stream while no KinesisReceivers are running.
 * In production, you'll want to switch to InitialPositionInStream.TRIM_HORIZON which will read up to 24 hours (Kinesis limit) of previous stream data 
 *  depending on the checkpoint frequency.
 * InitialPositionInStream.TRIM_HORIZON may lead to duplicate processing of records depending on the checkpoint frequency.
 * Record processing should be idempotent when possible.
 *
 * This code uses the DefaultAWSCredentialsProviderChain and searches for credentials in the following order of precedence: 
 *         Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 *         Java System Properties - aws.accessKeyId and aws.secretKey
 *         Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
 *         Instance profile credentials - delivered through the Amazon EC2 metadata service
 *
 * Usage: JavaKinesisWordCount <stream-name> <endpoint-url> <batch-interval>
 *         <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *         <endpoint-url> is the endpoint of the Kinesis service (ie. https://kinesis.us-east-1.amazonaws.com)
 *         <batch-interval> is the batch interval in milliseconds (ie. 1000ms)
 *
 * Example:
 *      $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *      $ export AWS_SECRET_KEY=<your-secret-key>
 *        $ bin/run-kinesis-example  \
 *            org.apache.spark.examples.streaming.JavaKinesisWordCount mySparkStream https://kinesis.us-east-1.amazonaws.com 1000
 *
 * There is a companion helper class called KinesisWordCountProducer which puts dummy data onto the Kinesis stream. 
 * Usage instructions for KinesisWordCountProducer are provided in the class definition.
 */
public final class JavaKinesisWordCount {
    private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");
    private static final Logger logger = Logger.getLogger(JavaKinesisWordCount.class);

    /**
     * Make the constructor private to enforce singleton
     */
    private JavaKinesisWordCount() {
    }

    public static void main(String[] args) {
        /**
         * Check that all required args were passed in.
         */
        if (args.length < 3) {
            System.err.println("Usage: JavaKinesisWordCount <stream-name> <kinesis-endpoint-url> <batch-interval>");
            System.exit(1);
        }

        /**
         * (This was lifted from the StreamingExamples.scala in order to avoid the dependency on the spark-examples artifact.)
         * Set reasonable logging levels for streaming if the user has not configured log4j.
         */
        boolean log4jInitialized = Logger.getRootLogger().getAllAppenders()
                .hasMoreElements();
        if (!log4jInitialized) {
            /** We first log something to initialize Spark's default logging, then we override the logging level. */
            Logger.getRootLogger()
                    .info("Setting log level to [ERROR] for streaming example."
                            + " To override add a custom log4j.properties to the classpath.");
            Logger.getRootLogger().setLevel(Level.ERROR);
            Logger.getLogger("org.apache.spark.examples.streaming").setLevel(Level.DEBUG);
        }

        /** Populate the appropriate variables from the given args */
        String stream = args[0];
        String endpoint = args[1];
        Integer batchIntervalMillis = Integer.valueOf(args[2]);

        /** Create a Kinesis client in order to determine the number of shards for the given stream */
        AmazonKinesisClient KinesisClient = new AmazonKinesisClient(
                new DefaultAWSCredentialsProviderChain());

        /** Determine the number of shards from the stream */
        int numShards = KinesisClient.describeStream(stream)
                .getStreamDescription().getShards().size();

        /** In this example, we're going to create 1 Kinesis Worker/Receiver/DStreams for each stream shard */ 
        int numStreams = numShards;

        /** Must add 1 more thread than the number of receivers or the output won't show properly from the driver */
        int numSparkThreads = numStreams + 1;

        /** Set the app name */
        String app = "KinesisWordCount";

        /** Setup the Spark config. */
        SparkConf sparkConfig = new SparkConf().setAppName(app).setMaster(
                "local[" + numSparkThreads + "]");

        /**
         * Set the batch interval.
         * Records will be pulled from the Kinesis stream and stored as a single DStream within Spark every batch interval.
         */
        Duration batchInterval = Milliseconds.apply(batchIntervalMillis);

        /**
         * It's recommended that you perform a Spark checkpoint between 5 and 10 times the batch interval. 
         * While this is the Spark checkpoint interval, we're going to use it for the Kinesis checkpoint interval, as well.
         */
        Duration checkpointInterval = batchInterval.$times(5);

        /** Setup the StreamingContext */
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

        /** Setup the checkpoint directory used by Spark Streaming */
        jssc.checkpoint("/tmp/checkpoint");

        /** Create the same number of Kinesis Receivers/DStreams as stream shards, then union them all */
        JavaDStream<byte[]> allStreams = KinesisUtils
                .createJavaStream(jssc, app, stream, endpoint, checkpointInterval.milliseconds(), 
                                    InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2());
        /** Set the checkpoint interval */
        allStreams.checkpoint(checkpointInterval);
        for (int i = 1; i < numStreams; i++) {
            /** Create a new Receiver/DStream for each stream shard */
            JavaDStream<byte[]> dStream = KinesisUtils
                    .createJavaStream(jssc, app, stream, endpoint, checkpointInterval.milliseconds(), 
                                        InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2());            
            /** Set the Spark checkpoint interval */
            dStream.checkpoint(checkpointInterval);

            /** Union with the existing streams */
            allStreams = allStreams.union(dStream);
        }

        /** This implementation uses the String-based KinesisRecordSerializer impl */
        final KinesisRecordSerializer<String> recordSerializer = new KinesisStringRecordSerializer();

        /**
          * Split each line of the union'd DStreams into multiple words using flatMap to produce the collection.
          * Convert lines of byte[] to multiple Strings by first converting to String, then splitting on WORD_SEPARATOR
          * We're caching the result here so that we can use it later without having to re-materialize the underlying RDDs.
          */
        JavaDStream<String> words = allStreams
                .flatMap(new FlatMapFunction<byte[], String>() {
                    /**
                     * Convert lines of byte[] to multiple words split by WORD_SEPARATOR
                     * @param byte array
                     * @return iterable of words split by WORD_SEPARATOR
                     */
                    @Override
                    public Iterable<String> call(byte[] line) {
                        return Lists.newArrayList(WORD_SEPARATOR.split(recordSerializer.deserialize(line)));
                    }
                }).cache();

        /** windowInterval must be a multiple of the batchInterval */
        Duration windowInterval = batchInterval.$times(5);

        /** slideInterval must be a multiple of the batchInterval */
        Duration slideInterval = batchInterval.$times(1);

        /**
         * Map each word to a (word, 1) tuple so we can reduce/aggregate later.
         * We're caching the result here so that we can use it later without having
         *     to re-materialize the underlying RDDs.
         */
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    /**
                     * Create the (word, 1) tuple
                     * @param word
                     * @return (word, 1) tuple
                     */
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        /**
         * Reduce/aggregate by key
         * We're caching the result here so that we can use it later without having
         *     to re-materialize the underlying RDDs.
         */
        JavaPairDStream<String, Integer> wordCountsByKey = wordCounts.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }).cache();

        /**
         * Reduce/aggregate by key for the given window.
         * We're using the inverse-function (left - right) optimization over the sliding window per the Window Operations described at the following url:
         *   http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations
         */
        JavaPairDStream<String, Integer> wordCountsByKeyAndWindow = wordCountsByKey.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }, windowInterval, slideInterval);

        /**
         * Sort and print the word counts by key and window.
         * This is an Output Operation and will materialize the DStream.
         */
        sortAndPrint("Word Counts By Key and Window", wordCountsByKeyAndWindow);

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateTotals =
                  new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override public Optional<Integer> call(List<Integer> newCounts, Optional<Integer> currentCount) {
                        Integer currentSum = 0;
                        if (currentCount.isPresent()) {
                            currentSum = currentCount.get();
                        }
                        Integer newSum = currentSum;

                        for (Integer newCount : newCounts) {
                            newSum += newCount;
                        }
                      return Optional.of(newSum);
                    }
                  };

        /**
         * Calculate the running totals using the updateTotals method.
         */
        JavaPairDStream<String, Integer> wordTotalsByKey = wordCountsByKey.updateStateByKey(updateTotals);

        /**
         * Sort and print the running word totals.
         * This is an Output Operation and will materialize the DStream.
         */
        sortAndPrint("Word Count Totals By Key", wordTotalsByKey);

        /** Start the streaming context and await termination */
        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * Sort and print the given dstream.
     * This is an Output Operation that will materialize the underlying DStream.
     * Everything up to this point is a lazy Transformation Operation.
     * 
     * @param description of the dstream for logging purposes
     * @param dstream to sort and print
     */
    private static void sortAndPrint(final String description, JavaPairDStream<String, Integer> dstream) {
         dstream.foreachRDD(
            new Function<JavaPairRDD<String, Integer>, Void>() {
               public Void call(JavaPairRDD<String, Integer> batch) {
                  JavaPairRDD<String, Integer> sortedBatch = batch.sortByKey(true);
                  logger.info(description);
                  for (Object wordCount: sortedBatch.collect()) {
                      logger.info(wordCount);
                  }

                  return null;
               }
            });
    }
}
