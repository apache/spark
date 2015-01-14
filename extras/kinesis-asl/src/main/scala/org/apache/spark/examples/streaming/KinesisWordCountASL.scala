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
import scala.util.Random
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Kinesis Spark Streaming WordCount example.
 *
 * See http://spark.apache.org/docs/latest/streaming-kinesis.html for more details on
 *   the Kinesis Spark Streaming integration.
 *
 * This example spins up 1 Kinesis Worker (Spark Streaming Receiver) per shard 
 *   for the given stream.
 * It then starts pulling from the last checkpointed sequence number of the given 
 *   <stream-name> and <endpoint-url>. 
 *
 * Valid endpoint urls:  http://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region
 * 
 * This code uses the DefaultAWSCredentialsProviderChain and searches for credentials
 *   in the following order of precedence:
 * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 * Java System Properties - aws.accessKeyId and aws.secretKey
 * Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
 * Instance profile credentials - delivered through the Amazon EC2 metadata service
 *
 * Usage: KinesisWordCountASL <stream-name> <endpoint-url>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <endpoint-url> is the endpoint of the Kinesis service
 *     (ie. https://kinesis.us-east-1.amazonaws.com)
 *
 * Example:
 *    $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *    $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ $SPARK_HOME/bin/run-example \
 *        org.apache.spark.examples.streaming.KinesisWordCountASL mySparkStream \
 *        https://kinesis.us-east-1.amazonaws.com
 *
 * 
 * Note that number of workers/threads should be 1 more than the number of receivers.
 * This leaves one thread available for actually processing the data.
 *
 * There is a companion helper class below called KinesisWordCountProducerASL which puts
 *   dummy data onto the Kinesis stream.
 * Usage instructions for KinesisWordCountProducerASL are provided in that class definition.
 */
private object KinesisWordCountASL extends Logging {
  def main(args: Array[String]) {
    /* Check that all required args were passed in. */
    if (args.length < 2) {
      System.err.println(
        """
          |Usage: KinesisWordCount <stream-name> <endpoint-url>
          |    <stream-name> is the name of the Kinesis stream
          |    <endpoint-url> is the endpoint of the Kinesis service
          |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    /* Populate the appropriate variables from the given args */
    val Array(streamName, endpointUrl) = args

    /* Determine the number of shards from the stream */
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards()
      .size()

    /* In this example, we're going to create 1 Kinesis Worker/Receiver/DStream for each shard. */
    val numStreams = numShards

    /* Setup the and SparkConfig and StreamingContext */
    /* Spark Streaming batch interval */
    val batchInterval = Milliseconds(2000)
    val sparkConfig = new SparkConf().setAppName("KinesisWordCount")
    val ssc = new StreamingContext(sparkConfig, batchInterval)

    /* Kinesis checkpoint interval.  Same as batchInterval for this example. */
    val kinesisCheckpointInterval = batchInterval

    /* Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards */
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, streamName, endpointUrl, kinesisCheckpointInterval,
          InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2)
    }

    /* Union all the streams */
    val unionStreams = ssc.union(kinesisStreams)

    /* Convert each line of Array[Byte] to String, split into words, and count them */
    val words = unionStreams.flatMap(byteArray => new String(byteArray)
      .split(" "))

    /* Map each word to a (word, 1) tuple so we can reduce/aggregate by key. */
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

    /* Print the first 10 wordCounts */
    wordCounts.print()

    /* Start the streaming context and await termination */
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * Usage: KinesisWordCountProducerASL <stream-name> <kinesis-endpoint-url>
 *     <recordsPerSec> <wordsPerRecord>
 *   <stream-name> is the name of the Kinesis stream (ie. mySparkStream)
 *   <kinesis-endpoint-url> is the endpoint of the Kinesis service
 *     (ie. https://kinesis.us-east-1.amazonaws.com)
 *   <records-per-sec> is the rate of records per second to put onto the stream
 *   <words-per-record> is the rate of records per second to put onto the stream
 *
 * Example:
 *    $ export AWS_ACCESS_KEY_ID=<your-access-key>
 *    $ export AWS_SECRET_KEY=<your-secret-key>
 *    $ $SPARK_HOME/bin/run-example \
 *         org.apache.spark.examples.streaming.KinesisWordCountProducerASL mySparkStream \
 *         https://kinesis.us-east-1.amazonaws.com 10 5
 */
private object KinesisWordCountProducerASL {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KinesisWordCountProducerASL <stream-name> <endpoint-url>" +
          " <records-per-sec> <words-per-record>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    /* Populate the appropriate variables from the given args */
    val Array(stream, endpoint, recordsPerSecond, wordsPerRecord) = args

    /* Generate the records and return the totals */
    val totals = generate(stream, endpoint, recordsPerSecond.toInt, wordsPerRecord.toInt)

    /* Print the array of (index, total) tuples */
    println("Totals")
    totals.foreach(total => println(total.toString()))
  }

  def generate(stream: String,
      endpoint: String,
      recordsPerSecond: Int,
      wordsPerRecord: Int): Seq[(Int, Int)] = {

    val MaxRandomInts = 10

    /* Create the Kinesis client */
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpoint)

    println(s"Putting records onto stream $stream and endpoint $endpoint at a rate of" +
      s" $recordsPerSecond records per second and $wordsPerRecord words per record");

    val totals = new Array[Int](MaxRandomInts)
    /* Put String records onto the stream per the given recordPerSec and wordsPerRecord */
    for (i <- 1 to 5) {

      /* Generate recordsPerSec records to put onto the stream */
      val records = (1 to recordsPerSecond.toInt).map { recordNum =>
        /* 
         *  Randomly generate each wordsPerRec words between 0 (inclusive)
         *  and MAX_RANDOM_INTS (exclusive) 
         */
        val data = (1 to wordsPerRecord.toInt).map(x => {
          /* Generate the random int */
          val randomInt = Random.nextInt(MaxRandomInts)

          /* Keep track of the totals */
          totals(randomInt) += 1

          randomInt.toString()
        }).mkString(" ")

        /* Create a partitionKey based on recordNum */
        val partitionKey = s"partitionKey-$recordNum"

        /* Create a PutRecordRequest with an Array[Byte] version of the data */
        val putRecordRequest = new PutRecordRequest().withStreamName(stream)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes()));

        /* Put the record onto the stream and capture the PutRecordResult */
        val putRecordResult = kinesisClient.putRecord(putRecordRequest);
      }

      /* Sleep for a second */
      Thread.sleep(1000)
      println("Sent " + recordsPerSecond + " records")
    }

    /* Convert the totals to (index, total) tuple */
    (0 to (MaxRandomInts - 1)).zip(totals)
  }
}

/** 
 *  Utility functions for Spark Streaming examples. 
 *  This has been lifted from the examples/ project to remove the circular dependency.
 */
private[streaming] object StreamingExamples extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
