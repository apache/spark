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

package org.apache.spark.streaming.kafka010

import java.io.File
import java.lang.{ Long => JLong }
import java.util.{ Arrays, HashMap => JHashMap, Map => JMap, UUID }
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
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
    // Set a timeout of 10 seconds that's going to be used to fetch topics/partitions from kafka.
    // Otherwise the poll timeout defaults to 2 minutes and causes test cases to run longer.
    .set("spark.streaming.kafka.consumer.poll.ms", "10000")

  private var ssc: StreamingContext = _
  private var testDir: File = _

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll() {
    super.beforeAll()
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll() {
    try {
      if (kafkaTestUtils != null) {
        kafkaTestUtils.teardown()
        kafkaTestUtils = null
      }
    } finally {
      super.afterAll()
    }
  }

  after {
    if (ssc != null) {
      ssc.stop(stopSparkContext = true)
    }
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }

  def getKafkaParams(extra: (String, Object)*): JHashMap[String, Object] = {
    val kp = new JHashMap[String, Object]()
    kp.put("bootstrap.servers", kafkaTestUtils.brokerAddress)
    kp.put("key.deserializer", classOf[StringDeserializer])
    kp.put("value.deserializer", classOf[StringDeserializer])
    kp.put("group.id", s"test-consumer-${Random.nextInt}-${System.currentTimeMillis}")
    extra.foreach(e => kp.put(e._1, e._2))
    kp
  }

  val preferredHosts = LocationStrategies.PreferConsistent

  test("basic stream receiving with multiple topics and smallest starting offset") {
    val topics = List("basic1", "basic2", "basic3")
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, data)
    }
    val offsets = Map(new TopicPartition("basic3", 0) -> 2L)
    // one topic is starting 2 messages later
    val expectedTotal = (data.values.sum * topics.size) - 2
    val kafkaParams = getKafkaParams("auto.offset.reset" -> "earliest")

    ssc = new StreamingContext(sparkConf, Milliseconds(1000))
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams.asScala, offsets))
    }
    val allReceived = new ConcurrentLinkedQueue[(String, String)]()

    // hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()
    val tf = stream.transform { rdd =>
      // Get the offset ranges in the RDD
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(r => (r.key, r.value))
    }

    tf.foreachRDD { rdd =>
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

    stream.foreachRDD { rdd =>
      allReceived.addAll(Arrays.asList(rdd.map(r => (r.key, r.value)).collect(): _*))
    }
    ssc.start()
    eventually(timeout(100.seconds), interval(1.second)) {
      assert(allReceived.size === expectedTotal,
        "didn't get expected number of messages, messages:\n" +
          allReceived.asScala.mkString("\n"))
    }
    ssc.stop()
  }

  test("pattern based subscription") {
    val topics = List("pat1", "pat2", "pat3", "advanced3")
    // Should match 3 out of 4 topics
    val pat = """pat\d""".r.pattern
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, data)
    }
    val offsets = Map(
      new TopicPartition("pat2", 0) -> 3L,
      new TopicPartition("pat3", 0) -> 4L)
    // 3 matching topics, two of which start a total of 7 messages later
    val expectedTotal = (data.values.sum * 3) - 7
    val kafkaParams = getKafkaParams("auto.offset.reset" -> "earliest")

    ssc = new StreamingContext(sparkConf, Milliseconds(1000))
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.SubscribePattern[String, String](pat, kafkaParams.asScala, offsets))
    }
    val allReceived = new ConcurrentLinkedQueue[(String, String)]()

    // hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()
    val tf = stream.transform { rdd =>
      // Get the offset ranges in the RDD
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(r => (r.key, r.value))
    }

    tf.foreachRDD { rdd =>
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

    stream.foreachRDD { rdd =>
      allReceived.addAll(Arrays.asList(rdd.map(r => (r.key, r.value)).collect(): _*))
    }
    ssc.start()
    eventually(timeout(100.seconds), interval(1.second)) {
      assert(allReceived.size === expectedTotal,
        "didn't get expected number of messages, messages:\n" +
          allReceived.asScala.mkString("\n"))
    }
    ssc.stop()
  }


  test("receiving from largest starting offset") {
    val topic = "latest"
    val topicPartition = new TopicPartition(topic, 0)
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    val kafkaParams = getKafkaParams("auto.offset.reset" -> "latest")
    val kc = new KafkaConsumer(kafkaParams)
    kc.assign(Arrays.asList(topicPartition))
    def getLatestOffset(): Long = {
      kc.seekToEnd(Arrays.asList(topicPartition))
      kc.position(topicPartition)
    }

    // Send some initial messages before starting context
    kafkaTestUtils.sendMessages(topic, data)
    eventually(timeout(10.seconds), interval(20.milliseconds)) {
      assert(getLatestOffset() > 3)
    }
    val offsetBeforeStart = getLatestOffset()
    kc.close()

    // Setup context and kafka stream with largest offset
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      val s = new DirectKafkaInputDStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams.asScala),
        new DefaultPerPartitionConfig(sparkConf))
      s.consumer.poll(0)
      assert(
        s.consumer.position(topicPartition) >= offsetBeforeStart,
        "Start offset not from latest"
      )
      s
    }

    val collectedData = new ConcurrentLinkedQueue[String]()
    stream.map { _.value }.foreachRDD { rdd =>
      collectedData.addAll(Arrays.asList(rdd.collect(): _*))
    }
    ssc.start()
    val newData = Map("b" -> 10)
    kafkaTestUtils.sendMessages(topic, newData)
    eventually(timeout(10.seconds), interval(50.milliseconds)) {
      collectedData.contains("b")
    }
    assert(!collectedData.contains("a"))
    ssc.stop()
  }


  test("creating stream by offset") {
    val topic = "offset"
    val topicPartition = new TopicPartition(topic, 0)
    val data = Map("a" -> 10)
    kafkaTestUtils.createTopic(topic)
    val kafkaParams = getKafkaParams("auto.offset.reset" -> "latest")
    val kc = new KafkaConsumer(kafkaParams)
    kc.assign(Arrays.asList(topicPartition))
    def getLatestOffset(): Long = {
      kc.seekToEnd(Arrays.asList(topicPartition))
      kc.position(topicPartition)
    }

    // Send some initial messages before starting context
    kafkaTestUtils.sendMessages(topic, data)
    eventually(timeout(10.seconds), interval(20.milliseconds)) {
      assert(getLatestOffset() >= 10)
    }
    val offsetBeforeStart = getLatestOffset()
    kc.close()

    // Setup context and kafka stream with largest offset
    kafkaParams.put("auto.offset.reset", "none")
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      val s = new DirectKafkaInputDStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Assign[String, String](
          List(topicPartition),
          kafkaParams.asScala,
          Map(topicPartition -> 11L)),
        new DefaultPerPartitionConfig(sparkConf))
      s.consumer.poll(0)
      assert(
        s.consumer.position(topicPartition) >= offsetBeforeStart,
        "Start offset not from latest"
      )
      s
    }

    val collectedData = new ConcurrentLinkedQueue[String]()
    stream.map(_.value).foreachRDD { rdd => collectedData.addAll(Arrays.asList(rdd.collect(): _*)) }
    ssc.start()
    val newData = Map("b" -> 10)
    kafkaTestUtils.sendMessages(topic, newData)
    eventually(timeout(10.seconds), interval(50.milliseconds)) {
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

    val kafkaParams = getKafkaParams("auto.offset.reset" -> "earliest")

    // Send data to Kafka
    def sendData(data: Seq[Int]) {
      val strings = data.map { _.toString}
      kafkaTestUtils.sendMessages(topic, strings.map { _ -> 1}.toMap)
    }

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    val kafkaStream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams.asScala))
    }
    val keyedStream = kafkaStream.map { r => "key" -> r.value.toInt }
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

    eventually(timeout(20.seconds), interval(50.milliseconds)) {
      assert(DirectKafkaStreamSuite.total.get === (1 to 10).sum)
    }

    ssc.stop()

    // Verify that offset ranges were generated
    val offsetRangesBeforeStop = getOffsetRanges(kafkaStream)
    assert(offsetRangesBeforeStop.size >= 1, "No offset ranges generated")
    assert(
      offsetRangesBeforeStop.head._2.forall { _.fromOffset === 0 },
      "starting offset not zero"
    )

    logInfo("====== RESTARTING ========")

    // Recover context from checkpoints
    ssc = new StreamingContext(testDir.getAbsolutePath)
    val recoveredStream =
      ssc.graph.getInputStreams().head.asInstanceOf[DStream[ConsumerRecord[String, String]]]

    // Verify offset ranges have been recovered
    val recoveredOffsetRanges = getOffsetRanges(recoveredStream).map { x => (x._1, x._2.toSet) }
    assert(recoveredOffsetRanges.size > 0, "No offset ranges recovered")
    val earlierOffsetRanges = offsetRangesBeforeStop.map { x => (x._1, x._2.toSet) }
    assert(
      recoveredOffsetRanges.forall { or =>
        earlierOffsetRanges.contains((or._1, or._2))
      },
      "Recovered ranges are not the same as the ones generated\n" +
        earlierOffsetRanges + "\n" + recoveredOffsetRanges
    )
    // Restart context, give more data and verify the total at the end
    // If the total is write that means each records has been received only once
    ssc.start()
    for (i <- (11 to 20).grouped(4)) {
      sendData(i)
    }

    eventually(timeout(20.seconds), interval(50.milliseconds)) {
      assert(DirectKafkaStreamSuite.total.get === (1 to 20).sum)
    }
    ssc.stop()
  }

    // Test to verify the offsets can be recovered from Kafka
  test("offset recovery from kafka") {
    val topic = "recoveryfromkafka"
    kafkaTestUtils.createTopic(topic)

    val kafkaParams = getKafkaParams(
      "auto.offset.reset" -> "earliest",
      ("enable.auto.commit", false: java.lang.Boolean)
    )

    val collectedData = new ConcurrentLinkedQueue[String]()
    val committed = new JHashMap[TopicPartition, OffsetAndMetadata]()

    // Send data to Kafka and wait for it to be received
    def sendDataAndWaitForReceive(data: Seq[Int]) {
      val strings = data.map { _.toString}
      kafkaTestUtils.sendMessages(topic, strings.map { _ -> 1}.toMap)
      eventually(timeout(10.seconds), interval(50.milliseconds)) {
        assert(strings.forall { collectedData.contains })
      }
    }

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    withClue("Error creating direct stream") {
      val kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams.asScala))
      kafkaStream.foreachRDD { (rdd: RDD[ConsumerRecord[String, String]], time: Time) =>
        val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val data = rdd.map(_.value).collect()
        collectedData.addAll(Arrays.asList(data: _*))
        kafkaStream.asInstanceOf[CanCommitOffsets]
          .commitAsync(offsets, (m: JMap[TopicPartition, OffsetAndMetadata], e: Exception) => {
            if (null != e) {
              logError("commit failed", e)
            } else {
              committed.putAll(m)
            }
          })
      }
    }
    ssc.start()
    // Send some data and wait for them to be received
    for (i <- (1 to 10).grouped(4)) {
      sendDataAndWaitForReceive(i)
    }
    ssc.stop()
    assert(! committed.isEmpty)
    val consumer = new KafkaConsumer[String, String](kafkaParams)
    consumer.subscribe(Arrays.asList(topic))
    consumer.poll(0)
    committed.asScala.foreach {
      case (k, v) =>
        // commits are async, not exactly once
        assert(v.offset > 0)
        assert(consumer.position(k) >= v.offset)
    }
  }


  test("Direct Kafka stream report input information") {
    val topic = "report-test"
    val data = Map("a" -> 7, "b" -> 9)
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, data)

    val totalSent = data.values.sum
    val kafkaParams = getKafkaParams("auto.offset.reset" -> "earliest")

    import DirectKafkaStreamSuite._
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val collector = new InputInfoCollector
    ssc.addStreamingListener(collector)

    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams.asScala))
    }

    val allReceived = new ConcurrentLinkedQueue[(String, String)]

    stream.map(r => (r.key, r.value))
      .foreachRDD { rdd => allReceived.addAll(Arrays.asList(rdd.collect(): _*)) }
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
    val kafkaStream = getDirectKafkaStream(topic, None, None)

    val input = Map(new TopicPartition(topic, 0) -> 50L, new TopicPartition(topic, 1) -> 50L)
    assert(kafkaStream.maxMessagesPerPartition(input).get ==
      Map(new TopicPartition(topic, 0) -> 10L, new TopicPartition(topic, 1) -> 10L))
  }

  test("maxMessagesPerPartition with no lag") {
    val topic = "maxMessagesPerPartition"
    val rateController = Some(new ConstantRateController(0, new ConstantEstimator(100), 100))
    val kafkaStream = getDirectKafkaStream(topic, rateController, None)

    val input = Map(new TopicPartition(topic, 0) -> 0L, new TopicPartition(topic, 1) -> 0L)
    assert(kafkaStream.maxMessagesPerPartition(input).isEmpty)
  }

  test("maxMessagesPerPartition respects max rate") {
    val topic = "maxMessagesPerPartition"
    val rateController = Some(new ConstantRateController(0, new ConstantEstimator(100), 1000))
    val ppc = Some(new PerPartitionConfig {
      def maxRatePerPartition(tp: TopicPartition) =
        if (tp.topic == topic && tp.partition == 0) {
          50
        } else {
          100
        }
    })
    val kafkaStream = getDirectKafkaStream(topic, rateController, ppc)

    val input = Map(new TopicPartition(topic, 0) -> 1000L, new TopicPartition(topic, 1) -> 1000L)
    assert(kafkaStream.maxMessagesPerPartition(input).get ==
      Map(new TopicPartition(topic, 0) -> 5L, new TopicPartition(topic, 1) -> 10L))
  }

  test("using rate controller") {
    val topic = "backpressure"
    kafkaTestUtils.createTopic(topic, 1)
    val kafkaParams = getKafkaParams("auto.offset.reset" -> "earliest")
    val executorKafkaParams = new JHashMap[String, Object](kafkaParams)
    KafkaUtils.fixKafkaParams(executorKafkaParams)

    val batchIntervalMilliseconds = 500
    val estimator = new ConstantEstimator(100)
    val messages = Map("foo" -> 5000)
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
      new DirectKafkaInputDStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams.asScala),
        new DefaultPerPartitionConfig(sparkConf)
      ) {
        override protected[streaming] val rateController =
          Some(new DirectKafkaRateController(id, estimator))
      }.map(r => (r.key, r.value))
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
      eventually(timeout(5.seconds), interval(10.milliseconds)) {
        // Assert that rate estimator values are used to determine maxMessagesPerPartition.
        // Funky "-" in message makes the complete assertion message read better.
        assert(collectedData.asScala.exists(_.size == expectedSize),
          s" - No arrays of size $expectedSize for rate $rate found in $dataToString")
      }
    }

    ssc.stop()
  }

  test("backpressure.initialRate should honor maxRatePerPartition") {
    backpressureTest(maxRatePerPartition = 1000, initialRate = 500, maxMessagesPerPartition = 250)
  }

  test("use backpressure.initialRate with backpressure") {
    backpressureTest(maxRatePerPartition = 300, initialRate = 1000, maxMessagesPerPartition = 150)
  }

  private def backpressureTest(
      maxRatePerPartition: Int,
      initialRate: Int,
      maxMessagesPerPartition: Int) = {

    val topic = UUID.randomUUID().toString
    val kafkaParams = getKafkaParams("auto.offset.reset" -> "earliest")
    val sparkConf = new SparkConf()
      // Safe, even with streaming, because we're using the direct API.
      // Using 1 core is useful to make the test more predictable.
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.backpressure.initialRate", initialRate.toString)
      .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition.toString)

    val messages = Map("foo" -> 5000)
    kafkaTestUtils.sendMessages(topic, messages)

    ssc = new StreamingContext(sparkConf, Milliseconds(500))

    val kafkaStream = withClue("Error creating direct stream") {
      new DirectKafkaInputDStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams.asScala),
        new DefaultPerPartitionConfig(sparkConf)
      )
    }
    kafkaStream.start()

    val input = Map(new TopicPartition(topic, 0) -> 1000L)

    assert(kafkaStream.maxMessagesPerPartition(input).get ==
      Map(new TopicPartition(topic, 0) -> maxMessagesPerPartition)) // we run for half a second

    kafkaStream.stop()
  }

  test("maxMessagesPerPartition with zero offset and rate equal to the specified" +
    " minimum with default 1") {
    val topic = "backpressure"
    val kafkaParams = getKafkaParams()
    val batchIntervalMilliseconds = 60000
    val sparkConf = new SparkConf()
      // Safe, even with streaming, because we're using the direct API.
      // Using 1 core is useful to make the test more predictable.
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.kafka.minRatePerPartition", "5")


    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(batchIntervalMilliseconds))
    val estimateRate = 1L
    val fromOffsets = Map(
      new TopicPartition(topic, 0) -> 0L,
      new TopicPartition(topic, 1) -> 0L,
      new TopicPartition(topic, 2) -> 0L,
      new TopicPartition(topic, 3) -> 0L
    )
    val kafkaStream = withClue("Error creating direct stream") {
      new DirectKafkaInputDStream[String, String](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, String](List(topic), kafkaParams.asScala),
        new DefaultPerPartitionConfig(sparkConf)
      ) {
        currentOffsets = fromOffsets
        override val rateController = Some(new ConstantRateController(id, null, estimateRate))
      }
    }

    val offsets = Map[TopicPartition, Long](
      new TopicPartition(topic, 0) -> 0,
      new TopicPartition(topic, 1) -> 100L,
      new TopicPartition(topic, 2) -> 200L,
      new TopicPartition(topic, 3) -> 300L
    )
    val result = kafkaStream.maxMessagesPerPartition(offsets)
    val expected = Map(
      new TopicPartition(topic, 0) -> 5L,
      new TopicPartition(topic, 1) -> 10L,
      new TopicPartition(topic, 2) -> 20L,
      new TopicPartition(topic, 3) -> 30L
    )
    assert(result.contains(expected), s"Number of messages per partition must be at least equal" +
      s" to the specified minimum")
  }

  /** Get the generated offset ranges from the DirectKafkaStream */
  private def getOffsetRanges[K, V](
      kafkaStream: DStream[ConsumerRecord[K, V]]): Seq[(Time, Array[OffsetRange])] = {
    kafkaStream.generatedRDDs.mapValues { rdd =>
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    }.toSeq.sortBy { _._1 }
  }

  private def getDirectKafkaStream(
      topic: String,
      mockRateController: Option[RateController],
      ppc: Option[PerPartitionConfig]) = {
    val batchIntervalMilliseconds = 100

    val sparkConf = new SparkConf()
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(batchIntervalMilliseconds))

    val kafkaParams = getKafkaParams("auto.offset.reset" -> "earliest")
    val ekp = new JHashMap[String, Object](kafkaParams)
    KafkaUtils.fixKafkaParams(ekp)

    val s = new DirectKafkaInputDStream[String, String](
      ssc,
      preferredHosts,
      new ConsumerStrategy[String, String] {
        def executorKafkaParams = ekp
        def onStart(currentOffsets: JMap[TopicPartition, JLong]): Consumer[String, String] = {
          val consumer = new KafkaConsumer[String, String](kafkaParams)
          val tps = List(new TopicPartition(topic, 0), new TopicPartition(topic, 1))
          consumer.assign(Arrays.asList(tps: _*))
          tps.foreach(tp => consumer.seek(tp, 0))
          consumer
        }
      },
      ppc.getOrElse(new DefaultPerPartitionConfig(sparkConf))
    ) {
        override protected[streaming] val rateController = mockRateController
    }
    // manual start necessary because we arent consuming the stream, just checking its state
    s.start()
    s
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
