package org.apache.spark.sql.kafka010

import org.apache.spark.SparkContext

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.test.TestSparkSession

// Trait to configure StreamTest for kafka continuous execution tests.
trait KafkaContinuousTest extends KafkaSourceTest {
  override val defaultTrigger = Trigger.Continuous(1000)
  override val defaultUseV2Sink = true

  // We need more than the default local[2] to be able to schedule all partitions simultaneously.
  override protected def createSparkSession = new TestSparkSession(
    new SparkContext(
      "local[10]",
      "continuous-stream-test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")))

  // In addition to setting the partitions in Kafka, we have to wait until the query has
  // reconfigured to the new count so the test framework can hook in properly.
  override protected def setTopicPartitions(
      topic: String, newCount: Int, query: StreamExecution) = {
    testUtils.addPartitions(topic, newCount)
    eventually(timeout(streamingTimeout)) {
      assert(
        query.lastExecution.logical.collectFirst {
          case DataSourceV2Relation(_, r: KafkaContinuousReader) => r
        }.exists(_.knownPartitions.size == newCount),
        s"query never reconfigured to $newCount partitions")
    }
  }

  test("ensure continuous stream is being used") {
    val query = spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "1")
      .load()

    testStream(query)(
      Execute(q => assert(q.isInstanceOf[ContinuousExecution]))
    )
  }
}
