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
import scala.util.Random

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
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
    val random = Random.nextInt
    val topics = Set(s"basic1-${random}", s"basic2-${random}", s"basic3-${random}")
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
    }
    basicDirectStreamTest(createStream(topics), topics)
  }

  test("basic stream receiving with multiple topics and smallest starting offset, using new Kafka" +
    " consumer API") {
    val random = Random.nextInt
    val topics = Set(s"basic1-${random}", s"basic2-${random}", s"basic3-${random}")
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
    }
    basicDirectStreamTest(createNewStream(topics), topics)
  }

  private def basicDirectStreamTest(stream: InputDStream[(String, String)], topics: Set[String]):
  Unit = {
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      kafkaTestUtils.sendMessages(t, data)
    }
    val totalSent = data.values.sum * topics.size

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

  private def createStream(topics: Set[String]): InputDStream[(String, String)] = {
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val kafkaParams: Map[String, String] = populateParams()
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    }
    stream
  }

  private def createNewStream(topics: Set[String]): InputDStream[(String, String)] = {
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val kafkaParams: Map[String, String] = populateNewParams()
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createNewDirectStream[String, String](
        ssc, kafkaParams, topics)
    }
    stream
  }

  private def populateParams(fromStart: Boolean = true): Map[String, String] = {
    val autoOffsetValue = if (fromStart) "smallest" else "largest"
    val kafkaParams = Map(
      "metadata.broker.list" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> autoOffsetValue
    )
    kafkaParams
  }

  private def populateNewParams(fromStart: Boolean = true): Map[String, String] = {
    val autoOffsetValue = if (fromStart) "earliest" else "latest"
    val kafkaParams = Map(
      "bootstrap.servers" -> kafkaTestUtils.brokerAddress,
      "auto.offset.reset" -> autoOffsetValue,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    kafkaParams
  }

  test("receiving from largest starting offset") {
    val topic = s"largest-${Random.nextInt}"
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    // Setup context and kafka stream with largest offset
    ssc = new StreamingContext(sparkConf, Milliseconds(200))

    val kafkaParams = populateParams(false)
    val kc = new KafkaCluster(kafkaParams)
    // Send some initial messages before starting context

    val topicPartition = TopicAndPartition(topic, 0)

    def getLatestOffset(): Long = {
      kc.getLatestLeaderOffsets(Set(topicPartition)).right.get(topicPartition).offset
    }

    eventually(timeout(10 seconds), interval(20 milliseconds)) {
      assert(getLatestOffset() > 3)
    }

    val offsetBeforeStart = getLatestOffset()

    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    }

    assert(
      stream.asInstanceOf[DirectKafkaInputDStream[_, _, _, _, _]]
        .fromOffsets(topicPartition) >= offsetBeforeStart,
      "Start offset not from latest"
    )

    assertDataValidity(topic, stream)
  }

  test("receiving from largest starting offset, using new Kafka consumer API") {
    val topic = s"largest-${Random.nextInt}"
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    // Setup context and kafka stream with largest offset
    ssc = new StreamingContext(sparkConf, Milliseconds(200))

    val kafkaParams = populateNewParams(false)
    val kc = new NewKafkaCluster(kafkaParams)
    // Send some initial messages before starting context

    val topicPartition = new TopicPartition(topic, 0)

    def getLatestOffset(): Long = {
      kc.getLatestOffsets(Set(topicPartition)).get(topicPartition).getOrElse(0)
    }

    eventually(timeout(10 seconds), interval(20 milliseconds)) {
      assert(getLatestOffset() > 3)
    }

    val offsetBeforeStart = getLatestOffset()

    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createNewDirectStream[String, String](
        ssc, kafkaParams, Set(topic))
    }

    assert(
      stream.asInstanceOf[NewDirectKafkaInputDStream[_, _, _]]
        .fromOffsets(topicPartition) >= offsetBeforeStart,
      "Start offset not from latest"
    )

    assertDataValidity(topic, stream)
  }

  def assertDataValidity(topic: String, stream: InputDStream[(String, String)]): Unit = {
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
    val topic = s"offset-${Random.nextInt}"
    val topicPartition = TopicAndPartition(topic, 0)
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    val kafkaParams = populateParams(false)
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

    assertValidityWhenCreatingStreamByOffset(topic, stream)
  }

  test("creating stream by offset, using new Kafka consumer API") {
    val topic = s"offset-${Random.nextInt}"
    val topicPartition = new TopicPartition(topic, 0)
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    val kafkaParams = populateNewParams(false)
    val kc = new NewKafkaCluster(kafkaParams)
    def getLatestOffset(): Long = {
      kc.getLatestOffsets(Set(topicPartition)).get(topicPartition).getOrElse(0)
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
      KafkaUtils.createNewDirectStream[String, String, String](
        ssc, kafkaParams, Map(topicPartition -> 11L),
        (m: ConsumerRecord[String, String]) => m.value())
    }
    assert(
      stream.asInstanceOf[NewDirectKafkaInputDStream[_, _, _]]
        .fromOffsets(topicPartition) >= offsetBeforeStart,
      "Start offset not from latest"
    )

    assertValidityWhenCreatingStreamByOffset(topic, stream)
  }

  // Test to verify the offset ranges can be recovered from the checkpoints
  def assertValidityWhenCreatingStreamByOffset(topic: String, stream: InputDStream[String]): Unit
  = {
    val collectedData = new ConcurrentLinkedQueue[String]()
    stream.foreachRDD { rdd => collectedData.addAll(Arrays.asList(rdd.collect(): _*)) }
    ssc.start()
    val newData = Map("b" -> 10)
    kafkaTestUtils.sendMessages(topic, newData)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      collectedData.contains("b")
    }
    assert(!collectedData.contains("a"))
  }

  test("offset recovery") {
    val kafkaParams: Map[String, String] = populateParams()
    val topic = s"recovery-${Random.nextInt}"
    kafkaTestUtils.createTopic(topic)

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    val kafkaStream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    }
    basicOffsetRecoveryTest(kafkaStream, topic, getOffsetRanges[String, String])
  }

  test("offset recovery with new Kafka consumer API") {
    val kafkaParams: Map[String, String] = populateNewParams()
    val topic = s"recovery-${Random.nextInt}"
    kafkaTestUtils.createTopic(topic)

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    val kafkaStream = withClue("Error creating direct stream") {
      KafkaUtils.createNewDirectStream[String, String](
        ssc, kafkaParams, Set(topic))
    }
    basicOffsetRecoveryTest(kafkaStream, topic, getNewOffsetRanges[String, String])
  }

  private def basicOffsetRecoveryTest(kafkaStream: InputDStream[(String, String)], topic: String,
    getOffsetRangesFunc: (DStream[(String, String)]) => Seq[(Time, Array[OffsetRange])]):
  Unit = {
    testDir = Utils.createTempDir()
    DirectKafkaStreamSuite.collectedData.clear()

    // Send data to Kafka and wait for it to be received
    def sendDataAndWaitForReceive(data: Seq[Int]) {
      val strings = data.map {
        _.toString
      }
      kafkaTestUtils.sendMessages(topic, strings.map {
        _ -> 1
      }.toMap)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        assert(strings.forall {
          DirectKafkaStreamSuite.collectedData.contains
        })
      }
    }

    val keyedStream = kafkaStream.map { v => "key" -> v._2.toInt }
    val stateStream = keyedStream.updateStateByKey { (values: Seq[Int], state: Option[Int]) =>
      Some(values.sum + state.getOrElse(0))
    }
    ssc.checkpoint(testDir.getAbsolutePath)

    // This is to collect the raw data received from Kafka
    kafkaStream.foreachRDD { (rdd: RDD[(String, String)], time: Time) =>
      val data = rdd.map { _._2 }.collect()
      DirectKafkaStreamSuite.collectedData.addAll(Arrays.asList(data: _*))
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
    val offsetRangesBeforeStop = getOffsetRangesFunc(kafkaStream)
    assert(offsetRangesBeforeStop.size >= 1, "No offset ranges generated")
    assert(
      offsetRangesBeforeStop.head._2.forall {
        _.fromOffset === 0
      },
      "starting offset not zero"
    )
    ssc.stop()
    logInfo("====== RESTARTING ========")

    // Recover context from checkpoints
    ssc = new StreamingContext(testDir.getAbsolutePath)
    val recoveredStream = ssc.graph.getInputStreams().head.asInstanceOf[DStream[(String, String)]]

    // Verify offset ranges have been recovered
    val recoveredOffsetRanges = getOffsetRangesFunc(recoveredStream)
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
    val topic = s"report-test-${Random.nextInt}"
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val kafkaParams: Map[String, String] = populateParams()
    kafkaTestUtils.createTopic(topic)
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    }

    basicReportInfoTest(stream, topic)
  }

  test("Direct Kafka stream report input information with new Kafka consumer API") {
    val topic = s"report-test-${Random.nextInt}"
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val kafkaParams: Map[String, String] = populateNewParams()
    kafkaTestUtils.createTopic(topic)
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createNewDirectStream[String, String](
        ssc, kafkaParams, Set(topic))
    }

    basicReportInfoTest(stream, topic)
  }

  private def basicReportInfoTest(stream: InputDStream[(String, String)], topic: String): Unit = {
    val data = Map("a" -> 7, "b" -> 9)
    kafkaTestUtils.sendMessages(topic, data)

    val totalSent = data.values.sum
    import DirectKafkaStreamSuite._

    val collector = new InputInfoCollector
    ssc.addStreamingListener(collector)

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

  test("using rate controller") {
    val topic = s"backpressure-${Random.nextInt}"
    val kafkaParams: Map[String, String] = populateParams()
    val batchIntervalMilliseconds = 100
    val estimator = new ConstantEstimator(100)

    kafkaTestUtils.createTopic(topic)

    val sparkConf = new SparkConf()
      // Safe, even with streaming, because we're using the direct API.
      // Using 1 core is useful to make the test more predictable.
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(batchIntervalMilliseconds))
    val topicPartition = TopicAndPartition(topic, 0)
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

    basicRateControllerTest(kafkaStream, topic, batchIntervalMilliseconds, estimator)

  }

  test("using rate controller using new Kafka consumer API") {
    val topic = s"backpressure-${Random.nextInt}"
    val kafkaParams: Map[String, String] = populateNewParams()
    val batchIntervalMilliseconds = 100
    val estimator = new ConstantEstimator(100)

    kafkaTestUtils.createTopic(topic)

    val sparkConf = new SparkConf()
      // Safe, even with streaming, because we're using the direct API.
      // Using 1 core is useful to make the test more predictable.
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(batchIntervalMilliseconds))
    val topicPartition = new TopicPartition(topic, 0)
    val kafkaStream = withClue("Error creating direct stream") {
      val kc = new NewKafkaCluster(kafkaParams)
      val messageHandler = (mmd: ConsumerRecord[String, String]) => (mmd.key, mmd.value)
      val m = kc.getEarliestOffsets(Set(topicPartition))

      new NewDirectKafkaInputDStream[String, String, (String, String)](
        ssc, kafkaParams, m, messageHandler) {
        override protected[streaming] val rateController =
          Some(new DirectKafkaRateController(id, estimator))
      }
    }
    basicRateControllerTest(kafkaStream, topic, batchIntervalMilliseconds, estimator)

  }

  private def basicRateControllerTest(kafkaStream: InputDStream[(String, String)], topic: String,
    batchIntervalMilliseconds: Int, estimator: ConstantEstimator): Unit = {

    val messageKeys = (1 to 200).map(_.toString)
    val messages = messageKeys.map((_, 1)).toMap
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
    // Send data to Kafka and wait for arrays of data to appear matching the rate.
    Seq(100, 50, 20).foreach { rate =>
      collectedData.clear() // Empty this buffer on each pass.
      estimator.updateRate(rate) // Set a new rate.
    // Expect blocks of data equal to "rate", scaled by the interval length in secs.
    val expectedSize = Math.round(rate * batchIntervalMilliseconds * 0.001)
      kafkaTestUtils.sendMessages(topic, messages)
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
    }.toSeq.sortBy {
      _._1
    }
  }

  /** Get the generated offset ranges from the DirectKafkaStream */
  private def getNewOffsetRanges[K, V](
    kafkaStream: DStream[(K, V)]): Seq[(Time, Array[OffsetRange])] = {
    kafkaStream.generatedRDDs.mapValues { rdd =>
      rdd.asInstanceOf[NewKafkaRDD[K, V, (K, V)]].offsetRanges
    }.toSeq.sortBy {
      _._1
    }
  }

}

object DirectKafkaStreamSuite {
  val collectedData = new ConcurrentLinkedQueue[String]()
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
