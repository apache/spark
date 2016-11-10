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
import java.util.Arrays
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.scheduler.rate.RateEstimator
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

    val allReceived = new ConcurrentLinkedQueue[(String, String)]()

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
    stream.foreachRDD { rdd => allReceived.addAll(Arrays.asList(rdd.collect(): _*)) }
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(allReceived.size === totalSent,
        "didn't get expected number of messages, messages:\n" +
          allReceived.asScala.mkString("\n"))
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

    val collectedData = new ConcurrentLinkedQueue[String]()
    stream.map { _._2 }.foreachRDD { rdd => collectedData.addAll(Arrays.asList(rdd.collect(): _*)) }
    ssc.start()
    val newData = Map("b" -> 10)
    kafkaTestUtils.sendMessages(topic, newData)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      collectedData.contains("b")
    }
    assert(!collectedData.contains("a"))
    ssc.stop()
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

    val collectedData = new ConcurrentLinkedQueue[String]()
    stream.foreachRDD { rdd => collectedData.addAll(Arrays.asList(rdd.collect(): _*)) }
    ssc.start()
    val newData = Map("b" -> 10)
    kafkaTestUtils.sendMessages(topic, newData)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      collectedData.contains("b")
    }
    assert(!collectedData.contains("a"))
    ssc.stop()
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
    def sendData(data: Seq[Int]) {
      val strings = data.map { _.toString}
      kafkaTestUtils.sendMessages(topic, strings.map { _ -> 1}.toMap)
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

    // This is ensure all the data is eventually receiving only once
    stateStream.foreachRDD { (rdd: RDD[(String, Int)]) =>
      rdd.collect().headOption.foreach { x =>
        DirectKafkaStreamSuite.total.set(x._2)
      }
    }
    ssc.start()

    // Send some data
    for (i <- (1 to 10).grouped(4)) {
      sendData(i)
    }

    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      assert(DirectKafkaStreamSuite.total.get === (1 to 10).sum)
    }

    ssc.stop()

    // Verify that offset ranges were generated
    // Since "offsetRangesAfterStop" will be used to compare with "recoveredOffsetRanges", we should
    // collect offset ranges after stopping. Otherwise, because new RDDs keep being generated before
    // stopping, we may not be able to get the latest RDDs, then "recoveredOffsetRanges" will
    // contain something not in "offsetRangesAfterStop".
    val offsetRangesAfterStop = getOffsetRanges(kafkaStream)
    assert(offsetRangesAfterStop.size >= 1, "No offset ranges generated")
    assert(
      offsetRangesAfterStop.head._2.forall { _.fromOffset === 0 },
      "starting offset not zero"
    )

    logInfo("====== RESTARTING ========")

    // Recover context from checkpoints
    ssc = new StreamingContext(testDir.getAbsolutePath)
    val recoveredStream = ssc.graph.getInputStreams().head.asInstanceOf[DStream[(String, String)]]

    // Verify offset ranges have been recovered
    val recoveredOffsetRanges = getOffsetRanges(recoveredStream).map { x => (x._1, x._2.toSet) }
    assert(recoveredOffsetRanges.size > 0, "No offset ranges recovered")
    val earlierOffsetRanges = offsetRangesAfterStop.map { x => (x._1, x._2.toSet) }
    assert(
      recoveredOffsetRanges.forall { or =>
        earlierOffsetRanges.contains((or._1, or._2))
      },
      "Recovered ranges are not the same as the ones generated\n" +
        s"recoveredOffsetRanges: $recoveredOffsetRanges\n" +
        s"earlierOffsetRanges: $earlierOffsetRanges"
    )
    // Restart context, give more data and verify the total at the end
    // If the total is write that means each records has been received only once
    ssc.start()
    for (i <- (11 to 20).grouped(4)) {
      sendData(i)
    }

    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      assert(DirectKafkaStreamSuite.total.get === (1 to 20).sum)
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

    val allReceived = new ConcurrentLinkedQueue[(String, String)]

    stream.foreachRDD { rdd => allReceived.addAll(Arrays.asList(rdd.collect(): _*)) }
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(allReceived.size === totalSent,
        "didn't get expected number of messages, messages:\n" +
          allReceived.asScala.mkString("\n"))

      // Calculate all the record number collected in the StreamingListener.
      assert(collector.numRecordsSubmitted.get() === totalSent)
      assert(collector.numRecordsStarted.get() === totalSent)
      assert(collector.numRecordsCompleted.get() === totalSent)
    }
    ssc.stop()
  }

  test("maxMessagesPerPartition with backpressure disabled") {
    val topic = "maxMessagesPerPartition"
    val kafkaStream = getDirectKafkaStream(topic, None)

    val input = Map(TopicAndPartition(topic, 0) -> 50L, TopicAndPartition(topic, 1) -> 50L)
    assert(kafkaStream.maxMessagesPerPartition(input).get ==
      Map(TopicAndPartition(topic, 0) -> 10L, TopicAndPartition(topic, 1) -> 10L))
  }

  test("maxMessagesPerPartition with no lag") {
    val topic = "maxMessagesPerPartition"
    val rateController = Some(new ConstantRateController(0, new ConstantEstimator(100), 100))
    val kafkaStream = getDirectKafkaStream(topic, rateController)

    val input = Map(TopicAndPartition(topic, 0) -> 0L, TopicAndPartition(topic, 1) -> 0L)
    assert(kafkaStream.maxMessagesPerPartition(input).isEmpty)
  }

  test("maxMessagesPerPartition respects max rate") {
    val topic = "maxMessagesPerPartition"
    val rateController = Some(new ConstantRateController(0, new ConstantEstimator(100), 1000))
    val kafkaStream = getDirectKafkaStream(topic, rateController)

    val input = Map(TopicAndPartition(topic, 0) -> 1000L, TopicAndPartition(topic, 1) -> 1000L)
    assert(kafkaStream.maxMessagesPerPartition(input).get ==
      Map(TopicAndPartition(topic, 0) -> 10L, TopicAndPartition(topic, 1) -> 10L))
  }

  test("using rate controller") {
    val topic = "backpressure"
    val topicPartitions = Set(TopicAndPartition(topic, 0), TopicAndPartition(topic, 1))
    kafkaTestUtils.createTopic(topic, 2)
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> "smallest"
    )

    val batchIntervalMilliseconds = 100
    val estimator = new ConstantEstimator(100)
    val messages = Map("foo" -> 200)
    kafkaTestUtils.sendMessages(topic, messages)

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
      val m = kc.getEarliestLeaderOffsets(topicPartitions)
        .fold(e => Map.empty[TopicAndPartition, Long], m => m.mapValues(lo => lo.offset))

      new DirectKafkaInputDStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc, kafkaParams, m, messageHandler) {
        override protected[streaming] val rateController =
          Some(new DirectKafkaRateController(id, estimator))
      }
    }

    val collectedData = new ConcurrentLinkedQueue[Array[String]]()

    // Used for assertion failure messages.
    def dataToString: String =
      collectedData.asScala.map(_.mkString("[", ",", "]")).mkString("{", ", ", "}")

    // This is to collect the raw data received from Kafka
    kafkaStream.foreachRDD { (rdd: RDD[(String, String)], time: Time) =>
      val data = rdd.map { _._2 }.collect()
      collectedData.add(data)
    }

    ssc.start()

    // Try different rate limits.
    // Wait for arrays of data to appear matching the rate.
    Seq(100, 50, 20).foreach { rate =>
      collectedData.clear()       // Empty this buffer on each pass.
      estimator.updateRate(rate)  // Set a new rate.
      // Expect blocks of data equal to "rate", scaled by the interval length in secs.
      val expectedSize = Math.round(rate * batchIntervalMilliseconds * 0.001)
      eventually(timeout(5.seconds), interval(batchIntervalMilliseconds.milliseconds)) {
        // Assert that rate estimator values are used to determine maxMessagesPerPartition.
        // Funky "-" in message makes the complete assertion message read better.
        assert(collectedData.asScala.exists(_.size == expectedSize),
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

  private def getDirectKafkaStream(topic: String, mockRateController: Option[RateController]) = {
    val batchIntervalMilliseconds = 100

    val sparkConf = new SparkConf()
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(batchIntervalMilliseconds))

    val earliestOffsets = Map(TopicAndPartition(topic, 0) -> 0L, TopicAndPartition(topic, 1) -> 0L)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
    new DirectKafkaInputDStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc, Map[String, String](), earliestOffsets, messageHandler) {
      override protected[streaming] val rateController = mockRateController
    }
  }
}

object DirectKafkaStreamSuite {
  val total = new AtomicLong(-1L)

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

private[streaming] class ConstantRateController(id: Int, estimator: RateEstimator, rate: Long)
  extends RateController(id, estimator) {
  override def publish(rate: Long): Unit = ()
  override def getLatestRate(): Long = rate
}
