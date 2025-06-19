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

package org.apache.spark.sql.execution.streaming

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.streaming.StreamTest

class WatermarkTrackerSuite extends StreamTest {

  import testImplicits._

  test("SPARK-50046 proper watermark advancement with dropped watermark nodes") {
    val inputStream1 = MemoryStream[Int]
    val inputStream2 = MemoryStream[Int]
    val inputStream3 = MemoryStream[Int]

    val df1 = inputStream1.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")

    val df2 = inputStream2.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "20 seconds")

    val df3 = inputStream3.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "30 seconds")

    val union = df1.union(df2).union(df3)

    testStream(union)(
      // just to ensure that executedPlan has watermark nodes for every stream.
      MultiAddData(
        (inputStream1, Seq(0)),
        (inputStream2, Seq(0)),
        (inputStream3, Seq(0))
      ),
      ProcessAllAvailable(),
      Execute { q =>
        val initialPlan = q.logicalPlan
        val executedPlan = q.lastExecution.executedPlan

        val tracker = WatermarkTracker(spark.conf, initialPlan)
        tracker.setWatermark(5)

        val delayMsToNodeId = executedPlan.collect {
          case e: EventTimeWatermarkExec => e.delayMs -> e.nodeId
        }.toMap

        def setupScenario(
            data: Map[Long, Seq[Long]])(fnToPruneSubtree: UnionExec => UnionExec): SparkPlan = {
          val eventTimeStatsMap = new mutable.HashMap[Long, EventTimeStatsAccum]()
          executedPlan.foreach {
            case e: EventTimeWatermarkExec =>
              eventTimeStatsMap.put(e.delayMs, e.eventTimeStats)

            case _ =>
          }

          data.foreach { case (delayMs, values) =>
            val stats = eventTimeStatsMap(delayMs)
            values.foreach { value =>
              stats.add(value)
            }
          }

          executedPlan.transform {
            case e: UnionExec => fnToPruneSubtree(e)
          }
        }

        def verifyWatermarkMap(expectation: Map[UUID, Option[Long]]): Unit = {
          expectation.foreach { case (nodeId, watermarkValue) =>
            assert(tracker.watermarkMap(nodeId) === watermarkValue,
            s"Watermark value for nodeId $nodeId is ${tracker.watermarkMap(nodeId)}, where " +
              s"we expect $watermarkValue")
          }
        }

        // Before SPARK-50046, WatermarkTracker simply assumes that the watermark node won't
        // be ever dropped, and the order of watermark nodes won't be changed. We don't find
        // a case which breaks this, but it had been happening for other operators (e.g.
        // PruneFilters), hence we would be better to guard against this in prior.

        // Scenario: We have three streams with watermark defined per stream. The query has
        // executed the first batch in the query run, and (due to some reason) Spark drops one
        // of subtrees. This should be considered like stream being a part of dropped subtree
        // had no data (because we do not know), hence watermark should not be advanced. But
        // before SPARK-50046, WatermarkTracker does not indicate there were watermark node being
        // dropped, hence watermark is advanced based on the calculation with remaining two
        // streams.

        val executedPlanFor1stBatch = setupScenario(
          Map(
            // watermark value for this node: 22 - 10 = 12
            10000L -> Seq(20000L, 21000L, 22000L),
            // watermark value for this node: 42 - 20 = 22
            20000L -> Seq(40000L, 41000L, 42000L),
            // watermark value for this node: 62 - 30 = 32
            30000L -> Seq(60000L, 61000L, 62000L)
          )
        ) { unionExec =>
          // drop the subtree which has watermark node having delay 10 seconds
          unionExec.copy(unionExec.children.drop(1))
        }

        tracker.updateWatermark(executedPlanFor1stBatch)

        // watermark hasn't advanced, hence taking default value.
        assert(tracker.currentWatermark === 5)

        verifyWatermarkMap(
          Map(
            delayMsToNodeId(10000L) -> None,
            delayMsToNodeId(20000L) -> Some(22000L),
            delayMsToNodeId(30000L) -> Some(32000L))
        )

        // NOTE: Before SPARK-50046, the above verification failed and the below verification works.
        // WatermarkTracker can't track the dropped node, hence it advances the watermark from the
        // remaining nodes, hence min(22, 32) = 22
        //
        // assert(tracker.currentWatermark === 22000)
        //
        // WatermarkTracker updates the map with shifted index. It should only update index 1 and
        // 2, but it updates 0 and 1.
        // verifyWatermarkMap(Map(0 -> Some(22000L), 1 -> Some(32000L)))

        // Scenario: after the first batch, the query has executed the second batch. In the second
        // batch, and (due to some reason) Spark only retains the middle of the subtrees. Before
        // SPARK-50046, WatermarkTracker only tracks the watermark nodes from physical plan with
        // index, hence the watermark node for the index 1 in logical plan is shifted to index 0,
        // updating the map incorrectly and also advancing the watermark. The correct behavior is,
        // the watermark node for the first stream has been dropped for both batches, hence
        // watermark must not be advanced.

        val executedPlanFor2ndBatch = setupScenario(
          Map(
            // watermark value for this node: 52 - 10 = 42
            10000L -> Seq(50000L, 51000L, 52000L),
            // watermark value for this node: 72 - 20 = 52
            20000L -> Seq(70000L, 71000L, 72000L),
            // watermark value for this node: 92 - 30 = 62
            30000L -> Seq(90000L, 91000L, 92000L)
          )
        ) { unionExec =>
          // only take the middle of the subtree, dropping remaining
          unionExec.copy(Seq(unionExec.children(1)))
        }

        tracker.updateWatermark(executedPlanFor2ndBatch)

        // watermark hasn't advanced, hence taking default value.
        assert(tracker.currentWatermark === 5)

        // WatermarkTracker properly updates the map for the middle of watermark node.
        verifyWatermarkMap(
          Map(
            delayMsToNodeId(10000L) -> None,
            delayMsToNodeId(20000L) -> Some(52000L),
            delayMsToNodeId(30000L) -> Some(32000L))
        )
      }
    )
  }
}
