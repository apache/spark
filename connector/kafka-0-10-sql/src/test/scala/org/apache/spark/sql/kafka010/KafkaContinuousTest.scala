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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.execution.datasources.v2.ContinuousScanExec
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.test.TestSparkSession

// Trait to configure StreamTest for kafka continuous execution tests.
trait KafkaContinuousTest extends KafkaSourceTest {
  override val defaultTrigger = Trigger.Continuous(1000)

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
        query.lastExecution.executedPlan.collectFirst {
          case scan: ContinuousScanExec
              if scan.stream.isInstanceOf[KafkaContinuousStream] =>
            scan.stream.asInstanceOf[KafkaContinuousStream]
        }.exists(_.knownPartitions.size == newCount),
        s"query never reconfigured to $newCount partitions")
    }
  }

  // Continuous processing tasks end asynchronously, so test that they actually end.
  private class TasksEndedListener extends SparkListener {
    val activeTaskIdCount = new AtomicInteger(0)

    override def onTaskStart(start: SparkListenerTaskStart): Unit = {
      activeTaskIdCount.incrementAndGet()
    }

    override def onTaskEnd(end: SparkListenerTaskEnd): Unit = {
      activeTaskIdCount.decrementAndGet()
    }
  }

  private val tasksEndedListener = new TasksEndedListener()

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sparkContext.addSparkListener(tasksEndedListener)
  }

  override def afterEach(): Unit = {
    eventually(timeout(streamingTimeout)) {
      assert(tasksEndedListener.activeTaskIdCount.get() == 0)
    }
    spark.sparkContext.removeSparkListener(tasksEndedListener)
    super.afterEach()
  }


  test("ensure continuous stream is being used") {
    val query = spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "1")
      .load()

    testStream(query)(
      makeSureGetOffsetCalled,
      Execute(q => assert(q.isInstanceOf[ContinuousExecution]))
    )
  }
}
