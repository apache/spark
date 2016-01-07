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
      KafkaSourceOffset(
        Map(TopicAndPartition(topic, lastMetadata.partition) -> lastMetadata.offset))
    }

    override def source: Source = kafkaSource
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
    kafkaParams = Map(
      "metadata.broker.list" -> testUtils.brokerAddress,
      "auto.offset.reset" -> "smallest"
    )
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  test("basic receiving") {
    val topic = "topic1"
    testUtils.createTopic(topic)

    val kafkaSource = KafkaSource(Set(topic), kafkaParams)
    val mapped = kafkaSource.toDS().map[Int]((kv: (Array[Byte], Array[Byte])) =>
      new String(kv._2).toInt + 1)

    testStream(mapped)(
      AddKafkaData(kafkaSource, topic, 1, 2, 3),
      CheckAnswer(2, 3, 4)
    )
  }
}
