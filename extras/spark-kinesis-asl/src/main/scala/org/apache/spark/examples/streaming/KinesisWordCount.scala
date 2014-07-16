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

package org.apache.spark.examples.streaming

import java.nio.ByteBuffer
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisStringRecordSerializer
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import scala.util.Random
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream

/**
 * Kinesis Spark Streaming WordCount example.
 *
 * See http://spark.apache.org/docs/latest/streaming-programming-guide.html for more details on the Kinesis Spark Streaming integration.
 *
 * This example spins up 1 Kinesis Worker (Spark Streaming Receivers) per shard of the given stream.
 * It then starts pulling from the tip of the given <stream-name> and <endpoint-url> at the given <batch-interval>.
 * Because we're pulling from the tip (InitialPositionInStream.LATEST), only new stream data will be picked up after the KinesisReceiver starts.
 * This could lead to missed records if data is added to the stream while no KinesisReceivers are running.
 * In production, you'll want to switch to InitialPositionInStream.TRIM_HORIZON which will read up to 24 hours (Kinesis limit) of previous stream data 
 *  depending on the checkpoint frequency.
 *
 * InitialPositionInStream.TRIM_HORIZON may lead to duplicate processing of records depending on the checkpoint frequency.
 * Record processing should be idempotent when possible.
 *
 * This code uses the DefaultAWSCredentialsProviderChain and searches for credentials in the following order of precedence:
 * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 * Java System Properties - aws.accessKeyId and aws.secretKey
 * Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
 * Instance profile credentials - delivered through the Amazon EC2 metadata service
 *
 * Usage: KinesisWordCount <stream-name> <endpoint-url> <batch-interval>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <endpoint-url> is the endpoint of the Kinesis service (ie. https://kinesis.us-east-1.amazonaws.com)
 *   <batch-interval> is the batch interval in millis (ie. 1000ms)
 *
 * Example:
 *      $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *      $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ bin/run-kinesis-example \
 *        org.apache.spark.examples.streaming.KinesisWordCount mySparkStream https://kinesis.us-east-1.amazonaws.com 100
 *
 * There is a companion helper class below called KinesisWordCountProducer which puts dummy data onto the Kinesis stream.
 * Usage instructions for KinesisWordCountProducer are provided in that class definition.
 */
object KinesisWordCount extends Logging {
  val WordSeparator = " "

  def main(args: Array[String]) {
/**
 * Check that all required args were passed in.
 */
    if (args.length < 3) {
      System.err.println("Usage: KinesisWordCount <stream-name> <endpoint-url> <batch-interval>")
      System.exit(1)
    }

    /**
     * (This was lifted from the StreamingExamples.scala in order to avoid the dependency on the spark-examples artifact.)
     * Set reasonable logging levels for streaming if the user has not configured log4j.
     */
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      /** We first log something to initialize Spark's default logging, then we override the logging level. */
      logInfo("Setting log level to [INFO] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")

      Logger.getRootLogger().setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.examples.streaming").setLevel(Level.DEBUG);
    }

    /** Populate the appropriate variables from the given args */
    val Array(stream, endpoint, batchIntervalMillisStr) = args
    val batchIntervalMillis = batchIntervalMillisStr.toInt

    /** Create a Kinesis client in order to determine the number of shards for the given stream */
    val KinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());

    /** Determine the number of shards from the stream */
    val numShards = KinesisClient.describeStream(stream).getStreamDescription().getShards().size()

    /** In this example, we're going to create 1 Kinesis Worker/Receiver/DStreams for each stream shard */
    val numStreams = numShards

    /** Must add 1 more thread than the number of receivers or the output won't show properly from the driver */
    val numSparkThreads = numStreams + 1

    /** Set the app name */
    val app = "KinesisWordCount"

    /** Setup the Spark config. */
    val sparkConfig = new SparkConf().setAppName(app).setMaster(s"local[$numSparkThreads]")

    /**
     * Set the batch interval.
     * Records will be pulled from the Kinesis stream and stored as a single DStream within Spark every batch interval.
     */
    val batchInterval = Milliseconds(batchIntervalMillis)

    /**
     * It's recommended that you perform a Spark checkpoint between 5 and 10 times the batch interval.
     * While this is the Spark checkpoint interval, we're going to use it for the Kinesis checkpoint interval, as well.
     */
    val checkpointInterval = batchInterval * 5

    /** Setup the StreamingContext */
    val ssc = new StreamingContext(sparkConfig, batchInterval)

    /** Setup the checkpoint directory used by Spark Streaming */
    ssc.checkpoint("/tmp/checkpoint");

    /** Create the same number of Kinesis Receivers/DStreams as stream shards, then union them all */
    var allStreams: DStream[Array[Byte]] = KinesisUtils.createStream(ssc, app, stream, endpoint, checkpointInterval.milliseconds,
      InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2)
      /** Set the checkpoint interval */
    allStreams.checkpoint(checkpointInterval)
    for (i <- 1 until numStreams) {
      /** Create a new Receiver/DStream for each stream shard */
      val dStream = KinesisUtils.createStream(ssc, app, stream, endpoint, checkpointInterval.milliseconds,
        InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2)
      /** Set the Spark checkpoint interval */
      dStream.checkpoint(checkpointInterval)

      /** Union with the existing streams */
      allStreams = allStreams.union(dStream)
    }

    /** This implementation uses the String-based KinesisRecordSerializer impl */
    val recordSerializer = new KinesisStringRecordSerializer()

    /**
     * Sort and print the given dstream.
     * This is an Output Operation that will materialize the underlying DStream.
     * Everything up to this point is a lazy Transformation Operation.
     * 
     * @param description of the dstream for logging purposes
     * @param dstream to sort and print
     */
    def sortAndPrint(description: String, dstream: DStream[(String,Int)]) = {
        dstream.foreachRDD((batch, endOfWindowTime) => {
            val sortedBatch = batch.sortByKey(true)
            logInfo(s"$description @ $endOfWindowTime")
            sortedBatch.collect().foreach(
                wordCount => logInfo(s"$wordCount"))
        })
    }

    /**
     * Split each line of the union'd DStreams into multiple words using flatMap to produce the collection.
     * Convert lines of Array[Byte] to multiple Strings by first converting to String, then splitting on WORD_SEPARATOR
     * We're caching the result here so that we can use it later without having to re-materialize the underlying RDDs.
     */
    val words = allStreams.flatMap(line => recordSerializer.deserialize(line).split(WordSeparator)).cache()

    /** windowInterval must be a multiple of the batchInterval */
    val windowInterval = batchInterval * 5

    /** slideInterval must be a multiple of the batchInterval */
    val slideInterval = batchInterval * 1

    /**
     * Map each word to a (word, 1) tuple so we can reduce/aggregate later.
     * We're caching the result here so that we can use it later without having
     * to re-materialize the underlying RDDs.
     */
    val wordCounts = words.map(word => (word, 1))

    /**
     * Reduce/aggregate by key.
     * We're caching the result here so that we can use it later without having
     * to re-materialize the underlying RDDs.
     */
    val wordCountsByKey = wordCounts.reduceByKey((left, right) => left + right)

    /**
     * Reduce/aggregate by key for the given window.
     * We're using the inverse-function (left - right) optimization over the sliding window per the Window Operations described at the following url:
     *   http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations
     */
    val wordCountsByKeyAndWindow = wordCountsByKey.reduceByKeyAndWindow((left, right) => left + right, (left, right) => left - right, windowInterval, slideInterval)

    /**
     * Sort and print the word counts by key and window.
     * This is an Output Operation and will materialize the DStream.
     * 
     */
    sortAndPrint("Word Counts By Key and Window", wordCountsByKeyAndWindow)

    /**
     * Update the running totals of words.
     *
     * @param sequence of new counts
     * @param current running total (could be None if no current count exists)
     */
    def updateTotals = (newCounts: Seq[Int], currentCounts: Option[Int]) => {
      val newCount = newCounts.foldLeft(0)((left, right) => left + right)
      val currentCount = currentCounts.getOrElse(0)
      Some(newCount + currentCount)
    }

    /**
     * Calculate the running totals using the updateTotals method.
     */
    val wordTotalsByKey = wordCountsByKey.updateStateByKey[Int](updateTotals)

    /**
     * Sort and print the running word totals.
     * This is an Output Operation and will materialize the DStream.
     */
    sortAndPrint("Word Count Totals By Key", wordTotalsByKey)

    /** Start the streaming context and await termination */
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * Usage: KinesisWordCountProducer <stream-name> <kinesis-endpoint-url> <recordsPerSec> <wordsPerRecord>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <kinesis-endpoint-url> is the endpoint of the Kinesis service (ie. https://kinesis.us-east-1.amazonaws.com)
 *   <records-per-sec> is the rate of records per second to put onto the stream
 *   <words-per-record> is the rate of records per second to put onto the stream
 *
 * Example:
 *      $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *      $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ bin/run-kinesis-example \
 *    org.apache.spark.examples.streaming.KinesisWordCountProducer mySparkStream https://kinesis.us-east-1.amazonaws.com 10 5
 */
private[streaming]
object KinesisWordCountProducer extends Logging {
  val MaxRandomInts = 10

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KinesisWordCountProducer <stream-name> <endpoint-url> <records-per-sec> <words-per-record>")
      System.exit(1)
    }

    /**
     * (This was lifted from the StreamingExamples.scala in order to avoid the dependency on the spark-examples artifact.)
     * Set reasonable logging levels for streaming if the user has not configured log4j.
     */
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      /** We first log something to initialize Spark's default logging, then we override the logging level. */
      logInfo("Setting log level to [INFO] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")

      Logger.getRootLogger().setLevel(Level.INFO)
      Logger.getLogger("org.apache.spark.examples.streaming").setLevel(Level.DEBUG);
    }

    /** Populate the appropriate variables from the given args */
    val Array(stream, endpoint, recordsPerSecond, wordsPerRecord) = args

    /** Generate the records and return the totals */
    val totals: Seq[(Int, Int)] = generate(stream, endpoint, recordsPerSecond.toInt, wordsPerRecord.toInt)

    logInfo("Totals")
    /** Print the array of (index, total) tuples */
    totals.foreach(total => logInfo(total.toString()))
  }

  def generate(stream: String, endpoint: String, recordsPerSecond: Int, wordsPerRecord: Int): Seq[(Int, Int)] = {
    val WORD_SEPARATOR = " "

    /** Create the Kinesis client */
    val KinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())

    logInfo(s"Putting records onto stream $stream and endpoint $endpoint at a rate of $recordsPerSecond records per second and $wordsPerRecord words per record");

    /** Create the String-based record serializer */
    val recordSerializer = new KinesisStringRecordSerializer()

    val totals = new Array[Int](MaxRandomInts)
    /** Put String records onto the stream per the given recordPerSec and wordsPerRecord */
    for (i <- 1 to 5) {
      /** Generate recordsPerSec records to put onto the stream */
      val records = (1 to recordsPerSecond.toInt).map { recordNum =>
        /** Randomly generate each wordsPerRec words between 0 (inclusive) and MAX_RANDOM_INTS (exclusive) */
        val data = (1 to wordsPerRecord.toInt).map(x => {
          /** Generate the random int */
          val randomInt = Random.nextInt(MaxRandomInts)

          /** Keep track of the totals */
          totals(randomInt) += 1

          /** Convert the Int to a String */
          randomInt.toString()
        })
          /** Create a String of randomInts separated by WORD_SEPARATOR */
          .mkString(WORD_SEPARATOR)

        /** Create a partitionKey based on recordNum */
        val partitionKey = s"partitionKey-$recordNum"

        /** Create a PutRecordRequest with an Array[Byte] version of the data */
        val putRecordRequest = new PutRecordRequest().withStreamName(stream).withPartitionKey(partitionKey)
          .withData(ByteBuffer.wrap(recordSerializer.serialize(data)));

        /** Put the record onto the stream and capture the PutRecordResult */
        val putRecordResult = KinesisClient.putRecord(putRecordRequest);

        logInfo(s"Successfully put record with partitionKey $partitionKey and shardId ${putRecordResult.getShardId()} and data $data")
      }

      /** Sleep for a second */
      Thread.sleep(1000)
    }

    /** Convert the totals to (index, total) tuple */
    (0 to (MaxRandomInts - 1)).zip(totals)
  }
}
