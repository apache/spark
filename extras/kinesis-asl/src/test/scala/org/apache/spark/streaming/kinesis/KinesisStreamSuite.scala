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

package org.apache.spark.streaming.kinesis

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class KinesisStreamSuite extends SparkFunSuite with KinesisSuiteHelper
  with Eventually with BeforeAndAfter with BeforeAndAfterAll {

  private val kinesisTestUtils = new KinesisTestUtils()

  // This is the name that KCL uses to save metadata to DynamoDB
  private val kinesisAppName = s"KinesisStreamSuite-${math.abs(Random.nextLong())}"

  private var ssc: StreamingContext = _
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    kinesisTestUtils.createStream()
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("KinesisStreamSuite") // Setting Spark app name to Kinesis app name
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    sc.stop()
    // Delete the stream as well as the table
    kinesisTestUtils.deleteStream()
    kinesisTestUtils.deleteDynamoDBTable(kinesisAppName)
  }

  before {
    // Delete the table so that each unit test can start from scratch without prior state
    kinesisTestUtils.deleteDynamoDBTable(kinesisAppName)
  }

  after {
    if (ssc != null) {
      ssc.stop(stopSparkContext = false)
      ssc = null
    }
  }

  test("KinesisUtils API") {
    ssc = new StreamingContext(sc, Seconds(1))
    // Tests the API, does not actually test data receiving
    val kinesisStream1 = KinesisUtils.createStream(ssc, "mySparkStream",
      "https://kinesis.us-west-2.amazonaws.com", Seconds(2),
      InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2)
    val kinesisStream2 = KinesisUtils.createStream(ssc, "myAppNam", "mySparkStream",
      "https://kinesis.us-west-2.amazonaws.com", "us-west-2",
      InitialPositionInStream.LATEST, Seconds(2), StorageLevel.MEMORY_AND_DISK_2)
    val kinesisStream3 = KinesisUtils.createStream(ssc, "myAppNam", "mySparkStream",
      "https://kinesis.us-west-2.amazonaws.com", "us-west-2",
      InitialPositionInStream.LATEST, Seconds(2), StorageLevel.MEMORY_AND_DISK_2,
      "awsAccessKey", "awsSecretKey")
  }


  /**
   * Test the stream by sending data to a Kinesis stream and receiving from it.
   * This by default ignore as the user may not have access to the
   */
  testOrIgnore("basic operation") {
    ssc = new StreamingContext(sc, Seconds(1))
    val aWSCredentials = KinesisTestUtils.getAWSCredentials()
    val stream = KinesisUtils.createStream(ssc, kinesisAppName, kinesisTestUtils.streamName,
      kinesisTestUtils.endpointUrl, kinesisTestUtils.regionName, InitialPositionInStream.LATEST,
      Seconds(10), StorageLevel.MEMORY_ONLY,
      aWSCredentials.getAWSAccessKeyId, aWSCredentials.getAWSSecretKey)

    val collected = new mutable.HashSet[Int] with mutable.SynchronizedSet[Int]
    stream.map { bytes => new String(bytes).toInt }.foreachRDD { rdd =>
      collected ++= rdd.collect()
      logInfo("Collected = " + rdd.collect().toSeq.mkString(", "))
    }
    ssc.start()

    val testData = 1 to 10
    eventually(timeout(120 seconds), interval(10 second)) {
      kinesisTestUtils.pushData(testData)
      assert(collected === testData.toSet, "\nData received does not match data sent")
    }
    ssc.stop()
  }
}
