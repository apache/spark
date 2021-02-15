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

package org.apache.spark.sql.kafka010

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag
import scala.util.Try

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.concurrent.TimeLimits.failAfter
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext, SparkException, TestUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.{MemoryStream, MemoryStreamBase}
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StringType, StructField, StructType}

abstract class KafkaSinkSuiteBase extends QueryTest with SharedSparkSession with KafkaTest {
  protected var testUtils: KafkaTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(
      withBrokerProps = Map("auto.create.topics.enable" -> "false"))
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    try {
      if (testUtils != null) {
        testUtils.teardown()
        testUtils = null
      }
    } finally {
      super.afterAll()
    }
  }

  private val topicId = new AtomicInteger(0)

  protected def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  protected def createKafkaReader(topic: String, includeHeaders: Boolean = false): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("subscribe", topic)
      .option("includeHeaders", includeHeaders.toString)
      .load()
  }
}

abstract class KafkaSinkStreamingSuiteBase extends KafkaSinkSuiteBase {
  import testImplicits._

  protected val streamingTimeout = 30.seconds

  protected def createMemoryStream(): MemoryStreamBase[String]
  protected def verifyResult(writer: StreamingQuery)(verifyFn: => Unit): Unit
  protected def defaultTrigger: Option[Trigger]

  test("streaming - write to kafka with topic field") {
    val input = createMemoryStream()
    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF(),
      withTopic = None,
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"'$topic' as topic", "value")

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
      .as[(Option[Int], Int)]
      .map(_._2)

    runAndVerifyValues(input, writer, reader)
  }

  test("streaming - write w/o topic field, with topic option") {
    val input = createMemoryStream()
    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append()))()

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
      .as[(Option[Int], Int)]
      .map(_._2)

    runAndVerifyValues(input, writer, reader)
  }

  test("streaming - topic field and topic option") {
    /* The purpose of this test is to ensure that the topic option
     * overrides the topic field. We begin by writing some data that
     * includes a topic field and value (e.g., 'foo') along with a topic
     * option. Then when we read from the topic specified in the option
     * we should see the data i.e., the data was written to the topic
     * option, and not to the topic in the data e.g., foo
     */
    val input = createMemoryStream()
    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append()))(
      withSelectExpr = "'foo' as topic", "value")

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(key AS INT)", "CAST(value AS INT)")
      .as[(Option[Int], Int)]
      .map(_._2)

    runAndVerifyValues(input, writer, reader)
  }

  test("streaming - write data with bad schema") {
    val input = createMemoryStream()
    val topic = newTopic()
    testUtils.createTopic(topic)

    assertWrongSchema(input, Seq("value as key", "value"),
      "topic option required when no 'topic' attribute is present")
    assertWrongSchema(input, Seq(s"'$topic' as topic", "value as key"),
      "required attribute 'value' not found")
  }

  test("streaming - write data with valid schema but wrong types") {
    val input = createMemoryStream()
    val topic = newTopic()
    testUtils.createTopic(topic)

    assertWrongSchema(input, Seq("CAST('1' as INT) as topic", "value"),
      "topic must be a(n) string")
    assertWrongSchema(input, Seq(s"'$topic' as topic", "CAST(value as INT) as value"),
      "value must be a(n) string or binary")
    assertWrongSchema(input, Seq(s"'$topic' as topic", "CAST(value as INT) as key", "value"),
      "key must be a(n) string or binary")
    assertWrongSchema(input, Seq(s"'$topic' as topic", "value", "value as partition"),
      "partition must be a(n) int")
  }

  test("streaming - write to non-existing topic") {
    val input = createMemoryStream()

    runAndVerifyException[StreamingQueryException](input, "job aborted") {
      createKafkaWriter(input.toDF(), withTopic = Some(newTopic()))()
    }
  }

  test("streaming - exception on config serializer") {
    val input = createMemoryStream()

    assertWrongOption(input, Map("kafka.key.serializer" -> "foo"),
      "kafka option 'key.serializer' is not supported")
    assertWrongOption(input, Map("kafka.value.serializer" -> "foo"),
      "kafka option 'value.serializer' is not supported")
  }

  protected def createKafkaWriter(
      input: DataFrame,
      withTopic: Option[String] = None,
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.length > 0) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format("kafka")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("kafka.max.block.ms", "5000")
        .queryName("kafkaStream")
      withTopic.foreach(stream.option("topic", _))
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
      defaultTrigger.foreach(stream.trigger(_))
    }
    stream.start()
  }

  private def runAndVerifyValues(
      input: MemoryStreamBase[String],
      writer: StreamingQuery,
      reader: Dataset[Int]): Unit = {
    try {
      input.addData("1", "2", "3", "4", "5")
      verifyResult(writer)(checkDatasetUnorderly(reader, 1, 2, 3, 4, 5))
      input.addData("6", "7", "8", "9", "10")
      verifyResult(writer)(checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6,
        7, 8, 9, 10))
    } finally {
      writer.stop()
    }
  }

  private def runAndVerifyException[T <: Exception : ClassTag](
      input: MemoryStreamBase[String],
      expectErrorMsg: String)(
      writerFn: => StreamingQuery): Unit = {
    var writer: StreamingQuery = null
    val ex: Exception = try {
      intercept[T] {
        writer = writerFn
        input.addData("1", "2", "3", "4", "5")
        input match {
          case _: MemoryStream[String] => writer.processAllAvailable()
          case _: ContinuousMemoryStream[String] =>
            eventually(timeout(streamingTimeout)) {
              assert(writer.exception.isDefined)
            }

            throw writer.exception.get
        }
      }
    } finally {
      if (writer != null) writer.stop()
    }
    TestUtils.assertExceptionMsg(ex, expectErrorMsg, ignoreCase = true)
  }

  private def assertWrongSchema(
      input: MemoryStreamBase[String],
      selectExpr: Seq[String],
      expectErrorMsg: String): Unit = {
    // just pick common exception of both micro-batch and continuous cases
    runAndVerifyException[Exception](input, expectErrorMsg) {
      createKafkaWriter(input.toDF())(
        withSelectExpr = selectExpr: _*)
    }
  }

  private def assertWrongOption(
      input: MemoryStreamBase[String],
      options: Map[String, String],
      expectErrorMsg: String): Unit = {
    // just pick common exception of both micro-batch and continuous cases
    runAndVerifyException[Exception](input, expectErrorMsg) {
      createKafkaWriter(input.toDF(), withOptions = options)()
    }
  }
}

class KafkaSinkMicroBatchStreamingSuite extends KafkaSinkStreamingSuiteBase {
  import testImplicits._

  override val streamingTimeout = 30.seconds

  override protected def createMemoryStream(): MemoryStreamBase[String] = MemoryStream[String]

  override protected def verifyResult(writer: StreamingQuery)(verifyFn: => Unit): Unit = {
    failAfter(streamingTimeout) {
      writer.processAllAvailable()
    }
    verifyFn
  }

  override protected def defaultTrigger: Option[Trigger] = None

  test("streaming - sink progress is produced") {
    /* ensure sink progress is correctly produced. */
    val input = MemoryStream[String]
    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Update()))()

    try {
      input.addData("1", "2", "3")
      verifyResult(writer) {
        assert(writer.recentProgress.exists(_.sink.numOutputRows == 3L))
      }
    } finally {
      writer.stop()
    }
  }
}

class KafkaContinuousSinkSuite extends KafkaSinkStreamingSuiteBase {
  import testImplicits._

  // We need more than the default local[2] to be able to schedule all partitions simultaneously.
  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  override protected def createMemoryStream(): MemoryStreamBase[String] = {
    ContinuousMemoryStream.singlePartition[String]
  }

  override protected def verifyResult(writer: StreamingQuery)(verifyFn: => Unit): Unit = {
    eventually(timeout(streamingTimeout), interval(5.seconds)) {
      verifyFn
    }
  }

  override protected def defaultTrigger: Option[Trigger] = Some(Trigger.Continuous(1000))

  test("generic - write big data with small producer buffer") {
    /* This test ensures that we understand the semantics of Kafka when
    * is comes to blocking on a call to send when the send buffer is full.
    * This test will configure the smallest possible producer buffer and
    * indicate that we should block when it is full. Thus, no exception should
    * be thrown in the case of a full buffer.
    */
    val topic = newTopic()
    testUtils.createTopic(topic, 1)
    val options = new java.util.HashMap[String, Object]
    options.put("bootstrap.servers", testUtils.brokerAddress)
    options.put("buffer.memory", "16384") // min buffer size
    options.put("block.on.buffer.full", "true")
    options.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    options.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val inputSchema = Seq(AttributeReference("value", BinaryType)())
    val data = new Array[Byte](15000) // large value
    val writeTask = new KafkaDataWriter(Some(topic), options, inputSchema)
    try {
      val fieldTypes: Array[DataType] = Array(BinaryType)
      val converter = UnsafeProjection.create(fieldTypes)
      val row = new SpecificInternalRow(fieldTypes)
      row.update(0, data)
      val iter = Seq.fill(1000)(converter.apply(row)).iterator
      iter.foreach(writeTask.write(_))
      writeTask.commit()
    } finally {
      writeTask.close()
    }
  }
}

abstract class KafkaSinkBatchSuiteBase extends KafkaSinkSuiteBase {
  import testImplicits._

  test("batch - write to kafka") {
    val topic = newTopic()
    testUtils.createTopic(topic)
    val data = Seq(
      Row(topic, "1", Seq(
        Row("a", "b".getBytes(UTF_8))
      )),
      Row(topic, "2", Seq(
        Row("c", "d".getBytes(UTF_8)),
        Row("e", "f".getBytes(UTF_8))
      )),
      Row(topic, "3", Seq(
        Row("g", "h".getBytes(UTF_8)),
        Row("g", "i".getBytes(UTF_8))
      )),
      Row(topic, "4", null),
      Row(topic, "5", Seq(
        Row("j", "k".getBytes(UTF_8)),
        Row("j", "l".getBytes(UTF_8)),
        Row("m", "n".getBytes(UTF_8))
      ))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(Seq(StructField("topic", StringType), StructField("value", StringType),
        StructField("headers", KafkaRecordToRowConverter.headersType)))
    )

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", topic)
      .mode("append")
      .save()
    checkAnswer(
      createKafkaReader(topic, includeHeaders = true).selectExpr(
        "CAST(value as STRING) value", "headers"
      ),
      Row("1", Seq(Row("a", "b".getBytes(UTF_8)))) ::
        Row("2", Seq(Row("c", "d".getBytes(UTF_8)), Row("e", "f".getBytes(UTF_8)))) ::
        Row("3", Seq(Row("g", "h".getBytes(UTF_8)), Row("g", "i".getBytes(UTF_8)))) ::
        Row("4", null) ::
        Row("5", Seq(
          Row("j", "k".getBytes(UTF_8)),
          Row("j", "l".getBytes(UTF_8)),
          Row("m", "n".getBytes(UTF_8)))) ::
        Nil
    )
  }

  def writeToKafka(df: DataFrame, topic: String, options: Map[String, String] = Map.empty): Unit = {
    df
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", topic)
      .options(options)
      .mode("append")
      .save()
  }

  def partitionsInTopic(topic: String): Set[Int] = {
    createKafkaReader(topic)
      .select("partition")
      .map(_.getInt(0))
      .collect()
      .toSet
  }

  test("batch - partition column and partitioner priorities") {
    val nrPartitions = 4
    val topic1 = newTopic()
    val topic2 = newTopic()
    val topic3 = newTopic()
    val topic4 = newTopic()
    testUtils.createTopic(topic1, nrPartitions)
    testUtils.createTopic(topic2, nrPartitions)
    testUtils.createTopic(topic3, nrPartitions)
    testUtils.createTopic(topic4, nrPartitions)
    val customKafkaPartitionerConf = Map(
      "kafka.partitioner.class" -> "org.apache.spark.sql.kafka010.TestKafkaPartitioner"
    )

    val df = (0 until 5).map(n => (topic1, s"$n", s"$n")).toDF("topic", "key", "value")

    // default kafka partitioner
    writeToKafka(df, topic1)
    val partitionsInTopic1 = partitionsInTopic(topic1)
    assert(partitionsInTopic1.size > 1)

    // custom partitioner (always returns 0) overrides default partitioner
    writeToKafka(df, topic2, customKafkaPartitionerConf)
    val partitionsInTopic2 = partitionsInTopic(topic2)
    assert(partitionsInTopic2.size == 1)
    assert(partitionsInTopic2.head == 0)

    // partition column overrides custom partitioner
    val dfWithCustomPartition = df.withColumn("partition", lit(2))
    writeToKafka(dfWithCustomPartition, topic3, customKafkaPartitionerConf)
    val partitionsInTopic3 = partitionsInTopic(topic3)
    assert(partitionsInTopic3.size == 1)
    assert(partitionsInTopic3.head == 2)

    // when the partition column value is null, it is ignored
    val dfWithNullPartitions = df.withColumn("partition", lit(null).cast(IntegerType))
    writeToKafka(dfWithNullPartitions, topic4)
    assert(partitionsInTopic(topic4) == partitionsInTopic1)
  }

  test("batch - null topic field value, and no topic option") {
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")
    val ex = intercept[SparkException] {
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .mode("append")
        .save()
    }
    TestUtils.assertExceptionMsg(ex, "null topic present in the data")
  }

  protected def testUnsupportedSaveModes(msg: (SaveMode) => Seq[String]): Unit = {
    val topic = newTopic()
    testUtils.createTopic(topic)
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")

    Seq(SaveMode.Ignore, SaveMode.Overwrite).foreach { mode =>
      val ex = intercept[AnalysisException] {
        df.write
          .format("kafka")
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .mode(mode)
          .save()
      }
      val errorChecks = msg(mode).map(m => Try(TestUtils.assertExceptionMsg(ex, m)))
      if (!errorChecks.exists(_.isSuccess)) {
        fail("Error messages not found in exception trace")
      }
    }
  }

  test("SPARK-20496: batch - enforce analyzed plans") {
    val inputEvents =
      spark.range(1, 1000)
        .select(to_json(struct("*")) as Symbol("value"))

    val topic = newTopic()
    testUtils.createTopic(topic)
    // used to throw UnresolvedException
    inputEvents.write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", topic)
      .mode("append")
      .save()
  }
}

class KafkaSinkBatchSuiteV1 extends KafkaSinkBatchSuiteBase {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "kafka")

  test("batch - unsupported save modes") {
    testUnsupportedSaveModes((mode) => s"Save mode ${mode.name} not allowed for Kafka" :: Nil)
  }
}

class KafkaSinkBatchSuiteV2 extends KafkaSinkBatchSuiteBase {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("batch - unsupported save modes") {
    testUnsupportedSaveModes((mode) =>
      Seq(s"cannot be written with ${mode.name} mode", "does not support truncate"))
  }

  test("generic - write big data with small producer buffer") {
    /* This test ensures that we understand the semantics of Kafka when
    * is comes to blocking on a call to send when the send buffer is full.
    * This test will configure the smallest possible producer buffer and
    * indicate that we should block when it is full. Thus, no exception should
    * be thrown in the case of a full buffer.
    */
    val topic = newTopic()
    testUtils.createTopic(topic, 1)
    val options = new java.util.HashMap[String, Object]
    options.put("bootstrap.servers", testUtils.brokerAddress)
    options.put("buffer.memory", "16384") // min buffer size
    options.put("block.on.buffer.full", "true")
    options.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    options.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val inputSchema = Seq(AttributeReference("value", BinaryType)())
    val data = new Array[Byte](15000) // large value
    val writeTask = new KafkaWriteTask(options, inputSchema, Some(topic))
    try {
      val fieldTypes: Array[DataType] = Array(BinaryType)
      val converter = UnsafeProjection.create(fieldTypes)
      val row = new SpecificInternalRow(fieldTypes)
      row.update(0, data)
      val iter = Seq.fill(1000)(converter.apply(row)).iterator
      writeTask.execute(iter)
    } finally {
      writeTask.close()
    }
  }
}

class TestKafkaPartitioner extends DefaultPartitioner {
  override def partition(
      topic: String,
      key: Any,
      keyBytes: Array[Byte],
      value: Any,
      valueBytes: Array[Byte],
      cluster: Cluster): Int = 0
}
