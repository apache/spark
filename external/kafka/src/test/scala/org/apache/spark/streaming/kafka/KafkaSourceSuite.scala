package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.test.SharedSQLContext


class KafkaSourceSuite extends StreamTest with SharedSQLContext {

  import testImplicits._

  private var testUtils: KafkaTestUtils = _
  private var kafkaParams: Map[String, String] = _

  override val streamingTimout = 30.seconds

  case class AddKafkaData(kafkaSource: KafkaSource, topic: String, data: Int*) extends AddData {
    override def addData(): Offset = {
      val sentMetadata = testUtils.sendMessages(topic, data.map{ _.toString}.toArray)
      val lastMetadata = sentMetadata.maxBy(_.offset)

      // Expected offset to ensure this data is read is last offset of this data + 1
      val offset = KafkaSourceOffset(
        Map(TopicAndPartition(topic, lastMetadata.partition) -> (lastMetadata.offset + 1)))
      logInfo(s"Added data, expected offset $offset")
      offset
    }

    override def source: Source = kafkaSource
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
    kafkaParams = Map("metadata.broker.list" -> testUtils.brokerAddress)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  test("basic receiving from latest offset with 1 topic and 1 partition") {
    val topic = "topic1"
    testUtils.createTopic(topic)


    // Add data in multiple rounds to the same topic and test whether
    for (i <- 0 until 5) {
      logInfo(s"Round $i")

      // Create Kafka source that reads from latest offset
      val kafkaSource = KafkaSource(Set(topic), kafkaParams)

      val mapped =
        kafkaSource
          .toDS()
          .map[Int]((kv: (Array[Byte], Array[Byte])) => new String(kv._2).toInt + 1)

      logInfo(s"Initial offsets: ${kafkaSource.initialOffsets}")

      testStream(mapped)(
        AddKafkaData(kafkaSource, topic, 1, 2, 3),
        CheckAnswer(2, 3, 4),
        StopStream ,
        DropBatches(1),         // Lose last batch in the sink
        StartStream,
        CheckAnswer(2, 3, 4),   // Should get the data back on recovery
        StopStream,
        AddKafkaData(kafkaSource, topic, 4, 5, 6),    // Add data when stream is stopped
        StartStream,
        CheckAnswer(2, 3, 4, 5, 6, 7),                // Should get the added data
        AddKafkaData(kafkaSource, topic, 7),
        CheckAnswer(2, 3, 4, 5, 6, 7, 8)
      )
    }
  }
}
