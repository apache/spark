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

package org.apache.spark.streaming.kafka

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler.rate.RateEstimator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.Utils

class DirectKafkaStreamSuite
  extends SparkFunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Eventually
  with Logging {
  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _
  private var testDir: File = _

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  after {
    if (ssc != null) {
      ssc.stop()
      sc = null
    }
    if (sc != null) {
      sc.stop()
    }
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }


  test("basic stream receiving with multiple topics and smallest starting offset") {
    val topics = Set("basic1", "basic2", "basic3")
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, data)
    }
    val totalSent = data.values.sum * topics.size
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "smallest"
    )

    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    }

    val allReceived =
      new ArrayBuffer[(String, String)] with mutable.SynchronizedBuffer[(String, String)]

    // hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()

    stream.transform { rdd =>
      // Get the offset ranges in the RDD
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        logInfo(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
      // For each partition, get size of the range in the partition,
      // and the number of items in the partition
        val off = offsetRanges(i)
        val all = iter.toSeq
        val partSize = all.size
        val rangeSize = off.untilOffset - off.fromOffset
        Iterator((partSize, rangeSize))
      }.collect

      // Verify whether number of elements in each partition
      // matches with the corresponding offset range
      collected.foreach { case (partSize, rangeSize) =>
        assert(partSize === rangeSize, "offset ranges are wrong")
      }
    }
    stream.foreachRDD { rdd => allReceived ++= rdd.collect() }
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(allReceived.size === totalSent,
        "didn't get expected number of messages, messages:\n" + allReceived.mkString("\n"))
    }
    ssc.stop()
  }

  test("receiving from largest starting offset") {
    val topic = "largest"
    val topicPartition = TopicAndPartition(topic, 0)
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "largest"
    )
    val kc = new KafkaCluster(kafkaParams)
    def getLatestOffset(): Long = {
      kc.getLatestLeaderOffsets(Set(topicPartition)).right.get(topicPartition).offset
    }

    // Send some initial messages before starting context
    kafkaTestUtils.sendMessages(topic, data)
    eventually(timeout(10 seconds), interval(20 milliseconds)) {
      assert(getLatestOffset() > 3)
    }
    val offsetBeforeStart = getLatestOffset()

    // Setup context and kafka stream with largest offset
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    }
    assert(
      stream.asInstanceOf[DirectKafkaInputDStream[_, _, _, _, _]]
        .fromOffsets(topicPartition) >= offsetBeforeStart,
      "Start offset not from latest"
    )

    val collectedData = new mutable.ArrayBuffer[String]() with mutable.SynchronizedBuffer[String]
    stream.map { _._2 }.foreachRDD { rdd => collectedData ++= rdd.collect() }
    ssc.start()
    val newData = Map("b" -> 10)
    kafkaTestUtils.sendMessages(topic, newData)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      collectedData.contains("b")
    }
    assert(!collectedData.contains("a"))
  }


  test("creating stream by offset") {
    val topic = "offset"
    val topicPartition = TopicAndPartition(topic, 0)
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "largest"
    )
    val kc = new KafkaCluster(kafkaParams)
    def getLatestOffset(): Long = {
      kc.getLatestLeaderOffsets(Set(topicPartition)).right.get(topicPartition).offset
    }

    // Send some initial messages before starting context
    kafkaTestUtils.sendMessages(topic, data)
    eventually(timeout(10 seconds), interval(20 milliseconds)) {
      assert(getLatestOffset() >= 10)
    }
    val offsetBeforeStart = getLatestOffset()

    // Setup context and kafka stream with largest offset
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
        ssc, kafkaParams, Map(topicPartition -> 11L),
        (m: MessageAndMetadata[String, String]) => m.message())
    }
    assert(
      stream.asInstanceOf[DirectKafkaInputDStream[_, _, _, _, _]]
        .fromOffsets(topicPartition) >= offsetBeforeStart,
      "Start offset not from latest"
    )

    val collectedData = new mutable.ArrayBuffer[String]() with mutable.SynchronizedBuffer[String]
    stream.foreachRDD { rdd => collectedData ++= rdd.collect() }
    ssc.start()
    val newData = Map("b" -> 10)
    kafkaTestUtils.sendMessages(topic, newData)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      collectedData.contains("b")
    }
    assert(!collectedData.contains("a"))
  }

  // Test to verify the offset ranges can be recovered from the checkpoints
  test("offset recovery") {
    val topic = "recovery"
    kafkaTestUtils.createTopic(topic)
    testDir = Utils.createTempDir()

    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "smallest"
    )

    // Send data to Kafka and wait for it to be received
    def sendDataAndWaitForReceive(data: Seq[Int]) {
      val strings = data.map { _.toString}
      kafkaTestUtils.sendMessages(topic, strings.map { _ -> 1}.toMap)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        assert(strings.forall { DirectKafkaStreamSuite.collectedData.contains })
      }
    }

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    val kafkaStream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    }
    val keyedStream = kafkaStream.map { v => "key" -> v._2.toInt }
    val stateStream = keyedStream.updateStateByKey { (values: Seq[Int], state: Option[Int]) =>
      Some(values.sum + state.getOrElse(0))
    }
    ssc.checkpoint(testDir.getAbsolutePath)

    // This is to collect the raw data received from Kafka
    kafkaStream.foreachRDD { (rdd: RDD[(String, String)], time: Time) =>
      val data = rdd.map { _._2 }.collect()
      DirectKafkaStreamSuite.collectedData.appendAll(data)
    }

    // This is ensure all the data is eventually receiving only once
    stateStream.foreachRDD { (rdd: RDD[(String, Int)]) =>
      rdd.collect().headOption.foreach { x => DirectKafkaStreamSuite.total = x._2 }
    }
    ssc.start()

    // Send some data and wait for them to be received
    for (i <- (1 to 10).grouped(4)) {
      sendDataAndWaitForReceive(i)
    }

    // Verify that offset ranges were generated
    val offsetRangesBeforeStop = getOffsetRanges(kafkaStream)
    assert(offsetRangesBeforeStop.size >= 1, "No offset ranges generated")
    assert(
      offsetRangesBeforeStop.head._2.forall { _.fromOffset === 0 },
      "starting offset not zero"
    )
    ssc.stop()
    logInfo("====== RESTARTING ========")

    // Recover context from checkpoints
    ssc = new StreamingContext(testDir.getAbsolutePath)
    val recoveredStream = ssc.graph.getInputStreams().head.asInstanceOf[DStream[(String, String)]]

    // Verify offset ranges have been recovered
    val recoveredOffsetRanges = getOffsetRanges(recoveredStream)
    assert(recoveredOffsetRanges.size > 0, "No offset ranges recovered")
    val earlierOffsetRangesAsSets = offsetRangesBeforeStop.map { x => (x._1, x._2.toSet) }
    assert(
      recoveredOffsetRanges.forall { or =>
        earlierOffsetRangesAsSets.contains((or._1, or._2.toSet))
      },
      "Recovered ranges are not the same as the ones generated"
    )
    // Restart context, give more data and verify the total at the end
    // If the total is write that means each records has been received only once
    ssc.start()
    sendDataAndWaitForReceive(11 to 20)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      assert(DirectKafkaStreamSuite.total === (1 to 20).sum)
    }
    ssc.stop()
  }

  test("Direct Kafka stream report input information") {
    val topic = "report-test"
    val data = Map("a" -> 7, "b" -> 9)
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    val totalSent = data.values.sum
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "smallest"
    )

    import DirectKafkaStreamSuite._
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val collector = new InputInfoCollector
    ssc.addStreamingListener(collector)

    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    }

    val allReceived =
      new ArrayBuffer[(String, String)] with mutable.SynchronizedBuffer[(String, String)]

    stream.foreachRDD { rdd => allReceived ++= rdd.collect() }
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(allReceived.size === totalSent,
        "didn't get expected number of messages, messages:\n" + allReceived.mkString("\n"))

      // Calculate all the record number collected in the StreamingListener.
      assert(collector.numRecordsSubmitted.get() === totalSent)
      assert(collector.numRecordsStarted.get() === totalSent)
      assert(collector.numRecordsCompleted.get() === totalSent)
    }
    ssc.stop()
  }

  test("using rate controller") {
    val topic = "backpressure"
    val topicPartition = TopicAndPartition(topic, 0)
    kafkaTestUtils.createTopic(topic)
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "smallest"
    )

    val batchIntervalMilliseconds = 100
    val estimator = new ConstantEstimator(100)
    val messageKeys = (1 to 200).map(_.toString)
    val messages = messageKeys.map((_, 1)).toMap

    val sparkConf = new SparkConf()
      // Safe, even with streaming, because we're using the direct API.
      // Using 1 core is useful to make the test more predictable.
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(batchIntervalMilliseconds))

    val kafkaStream = withClue("Error creating direct stream") {
      val kc = new KafkaCluster(kafkaParams)
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
      val m = kc.getEarliestLeaderOffsets(Set(topicPartition))
        .fold(e => Map.empty[TopicAndPartition, Long], m => m.mapValues(lo => lo.offset))

      new DirectKafkaInputDStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, m, messageHandler) {
        override protected[streaming] val rateController =
          Some(new DirectKafkaRateController(id, estimator))
      }
    }

    val collectedData =
      new mutable.ArrayBuffer[Array[String]]() with mutable.SynchronizedBuffer[Array[String]]

    // Used for assertion failure messages.
    def dataToString: String =
      collectedData.map(_.mkString("[", ",", "]")).mkString("{", ", ", "}")

    // This is to collect the raw data received from Kafka
    kafkaStream.foreachRDD { (rdd: RDD[(String, String)], time: Time) =>
      val data = rdd.map { _._2 }.collect()
      collectedData += data
    }

    ssc.start()

    // Try different rate limits.
    // Send data to Kafka and wait for arrays of data to appear matching the rate.
    Seq(100, 50, 20).foreach { rate =>
      collectedData.clear()       // Empty this buffer on each pass.
      estimator.updateRate(rate)  // Set a new rate.
      // Expect blocks of data equal to "rate", scaled by the interval length in secs.
      val expectedSize = Math.round(rate * batchIntervalMilliseconds * 0.001)
      kafkaTestUtils.sendMessages(topic, messages)
      eventually(timeout(5.seconds), interval(batchIntervalMilliseconds.milliseconds)) {
        // Assert that rate estimator values are used to determine maxMessagesPerPartition.
        // Funky "-" in message makes the complete assertion message read better.
        assert(collectedData.exists(_.size == expectedSize),
          s" - No arrays of size $expectedSize for rate $rate found in $dataToString")
      }
    }

    ssc.stop()
  }

  /** Get the generated offset ranges from the DirectKafkaStream */
  private def getOffsetRanges[K, V](
      kafkaStream: DStream[(K, V)]): Seq[(Time, Array[OffsetRange])] = {
    kafkaStream.generatedRDDs.mapValues { rdd =>
      rdd.asInstanceOf[KafkaRDD[K, V, _, _, (K, V)]].offsetRanges
    }.toSeq.sortBy { _._1 }
  }
}

object DirectKafkaStreamSuite {
  val collectedData = new mutable.ArrayBuffer[String]() with mutable.SynchronizedBuffer[String]
  @volatile var total = -1L

  class InputInfoCollector extends StreamingListener {
    val numRecordsSubmitted = new AtomicLong(0L)
    val numRecordsStarted = new AtomicLong(0L)
    val numRecordsCompleted = new AtomicLong(0L)

    override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
      numRecordsSubmitted.addAndGet(batchSubmitted.batchInfo.numRecords)
    }

    override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
      numRecordsStarted.addAndGet(batchStarted.batchInfo.numRecords)
    }

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      numRecordsCompleted.addAndGet(batchCompleted.batchInfo.numRecords)
    }
  }
}

private[streaming] class ConstantEstimator(@volatile private var rate: Long)
  extends RateEstimator {

  def updateRate(newRate: Long): Unit = {
    rate = newRate
  }

  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double] = Some(rate)
}

