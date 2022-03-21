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

package org.apache.spark.sql.streaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.Assertions

import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.streaming.{MemoryStream, StateStoreRestoreExec, StateStoreSaveExec}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.streaming.util.StatefulOpClusteredDistributionTestHelper
import org.apache.spark.util.Utils

class StreamingAggregationDistributionSuite extends StreamTest
  with StatefulOpClusteredDistributionTestHelper with Assertions {

  import testImplicits._

  test("SPARK-38204: streaming aggregation should require StatefulOpClusteredDistribution " +
    "from children") {

    val input = MemoryStream[Int]
    val df1 = input.toDF().select('value as 'key1, 'value * 2 as 'key2, 'value * 3 as 'value)
    val agg = df1.repartition('key1).groupBy('key1, 'key2).agg(count('*))

    testStream(agg, OutputMode.Update())(
      AddData(input, 1, 1, 2, 3, 4),
      CheckAnswer((1, 2, 2), (2, 4, 1), (3, 6, 1), (4, 8, 1)),
      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        // verify state store restore/save
        val stateStoreOps = query.lastExecution.executedPlan.collect {
          case s: StateStoreRestoreExec => s
          case s: StateStoreSaveExec => s
        }

        assert(stateStoreOps.nonEmpty)
        stateStoreOps.foreach { stateOp =>
          assert(requireStatefulOpClusteredDistribution(stateOp, Seq(Seq("key1", "key2")),
            numPartitions))
          assert(hasDesiredHashPartitioningInChildren(stateOp, Seq(Seq("key1", "key2")),
            numPartitions))
        }

        // verify aggregations in between, except partial aggregation
        val allAggregateExecs = query.lastExecution.executedPlan.collect {
          case a: BaseAggregateExec => a
        }

        val aggregateExecsWithoutPartialAgg = allAggregateExecs.filter {
          _.requiredChildDistribution.head != UnspecifiedDistribution
        }

        // We expect single partial aggregation - remaining agg execs should have child producing
        // expected output partitioning.
        assert(allAggregateExecs.length - 1 === aggregateExecsWithoutPartialAgg.length)

        // For aggregate execs, we make sure output partitioning of the children is same as
        // we expect, HashPartitioning with clustering keys & number of partitions.
        aggregateExecsWithoutPartialAgg.foreach { aggr =>
          assert(hasDesiredHashPartitioningInChildren(aggr, Seq(Seq("key1", "key2")),
            numPartitions))
        }
      }
    )
  }

  test("SPARK-38204: streaming aggregation should require ClusteredDistribution " +
    "from children if the query starts from checkpoint in prior to 3.3") {

    val inputData = MemoryStream[Int]
    val df1 = inputData.toDF().select('value as 'key1, 'value * 2 as 'key2, 'value * 3 as 'value)
    val agg = df1.repartition('key1).groupBy('key1, 'key2).agg(count('*))

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-3.2.0-streaming-aggregate-with-repartition/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(3)
    inputData.addData(3, 2)

    testStream(agg, Update)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION.key -> "true")),

      // scalastyle:off line.size.limit
      /*
        Note: The checkpoint was generated using the following input in Spark version 3.2.0
        AddData(inputData, 3),
        CheckLastBatch((3, 6, 1)),
        AddData(inputData, 3, 2),
        CheckLastBatch((3, 6, 2), (2, 4, 1))

        Note2: The following plans are the physical plans of the query in older Spark versions
        The physical plans around StateStoreRestore and StateStoreSave are quite similar,
        especially shuffles being injected are same. That said, verifying with checkpoint being
        built with Spark 3.2.0 would verify the following versions as well.

        A. Spark 3.2.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@61a581c0, org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$1968/1468582588@325b0006
        +- *(4) HashAggregate(keys=[key1#3, key2#4], functions=[count(1)], output=[key1#3, key2#4, count(1)#13L])
           +- StateStoreSave [key1#3, key2#4], state info [ checkpoint = file:/blabla/state, runId = 2bd7d18c-73b2-49a2-b2aa-1835162f9186, opId = 0, ver = 1, numPartitions = 5], Update, 0, 2
              +- *(3) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#47L])
                 +- StateStoreRestore [key1#3, key2#4], state info [ checkpoint = file:/blabla/state, runId = 2bd7d18c-73b2-49a2-b2aa-1835162f9186, opId = 0, ver = 1, numPartitions = 5], 2
                    +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#47L])
                       +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[partial_count(1)], output=[key1#3, key2#4, count#47L])
                          +- Exchange hashpartitioning(key1#3, 5), REPARTITION_BY_COL, [id=#220]
                             +- *(1) Project [value#1 AS key1#3, (value#1 * 2) AS key2#4]
                                +- MicroBatchScan[value#1] MemoryStreamDataSource

        B. Spark 3.1.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@53602363
        +- *(4) HashAggregate(keys=[key1#3, key2#4], functions=[count(1)], output=[key1#3, key2#4, count(1)#13L])
           +- StateStoreSave [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-178e9eaf-b527-499c-8eb6-c9e734f9fdfc/state, runId = 9c7e8635-41ab-4141-9f46-7ab473c58560, opId = 0, ver = 1, numPartitions = 5], Update, 0, 2
              +- *(3) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#47L])
                 +- StateStoreRestore [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-178e9eaf-b527-499c-8eb6-c9e734f9fdfc/state, runId = 9c7e8635-41ab-4141-9f46-7ab473c58560, opId = 0, ver = 1, numPartitions = 5], 2
                    +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#47L])
                       +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[partial_count(1)], output=[key1#3, key2#4, count#47L])
                          +- Exchange hashpartitioning(key1#3, 5), REPARTITION, [id=#222]
                             +- *(1) Project [value#1 AS key1#3, (value#1 * 2) AS key2#4]
                                +- *(1) Project [value#1]
                                   +- MicroBatchScan[value#1] MemoryStreamDataSource

        C. Spark 3.0.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@33379044
        +- *(4) HashAggregate(keys=[key1#3, key2#4], functions=[count(1)], output=[key1#3, key2#4, count(1)#13L])
           +- StateStoreSave [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-83497e04-657c-4cad-b532-f433b1532302/state, runId = 1a650994-486f-4f32-92d9-f7c05d49d0a0, opId = 0, ver = 1, numPartitions = 5], Update, 0, 2
              +- *(3) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#47L])
                 +- StateStoreRestore [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-83497e04-657c-4cad-b532-f433b1532302/state, runId = 1a650994-486f-4f32-92d9-f7c05d49d0a0, opId = 0, ver = 1, numPartitions = 5], 2
                    +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#47L])
                       +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[partial_count(1)], output=[key1#3, key2#4, count#47L])
                          +- Exchange hashpartitioning(key1#3, 5), false, [id=#104]
                             +- *(1) Project [value#1 AS key1#3, (value#1 * 2) AS key2#4]
                                +- *(1) Project [value#1]
                                   +- MicroBatchScan[value#1] MemoryStreamDataSource

        D. Spark 2.4.0
        *(4) HashAggregate(keys=[key1#3, key2#4], functions=[count(1)], output=[key1#3, key2#4, count(1)#13L])
        +- StateStoreSave [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-c4fd5b1f-18e0-4433-ac7a-00df93464b49/state, runId = 89bfe27b-da33-4a75-9f36-97717c137b2a, opId = 0, ver = 1, numPartitions = 5], Update, 0, 2
           +- *(3) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#42L])
              +- StateStoreRestore [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-c4fd5b1f-18e0-4433-ac7a-00df93464b49/state, runId = 89bfe27b-da33-4a75-9f36-97717c137b2a, opId = 0, ver = 1, numPartitions = 5], 2
                 +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[merge_count(1)], output=[key1#3, key2#4, count#42L])
                    +- *(2) HashAggregate(keys=[key1#3, key2#4], functions=[partial_count(1)], output=[key1#3, key2#4, count#42L])
                       +- Exchange hashpartitioning(key1#3, 5)
                          +- *(1) Project [value#47 AS key1#3, (value#47 * 2) AS key2#4]
                             +- *(1) Project [value#47]
                                +- *(1) ScanV2 MemoryStreamDataSource$[value#47]
       */
      // scalastyle:on line.size.limit

      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 6, 3), (2, 4, 2), (1, 2, 1)),

      Execute { query =>
        val executedPlan = query.lastExecution.executedPlan
        assert(!executedPlan.conf.getConf(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION))

        val numPartitions = query.lastExecution.numStateStores

        // verify state store restore/save
        val stateStoreOps = executedPlan.collect {
          case s: StateStoreRestoreExec => s
          case s: StateStoreSaveExec => s
        }

        assert(stateStoreOps.nonEmpty)
        stateStoreOps.foreach { stateOp =>
          assert(requireClusteredDistribution(stateOp, Seq(Seq("key1", "key2")),
            Some(numPartitions)))
          assert(hasDesiredHashPartitioningInChildren(stateOp, Seq(Seq("key1")),
            numPartitions))
        }

        // verify aggregations in between, except partial aggregation
        val allAggregateExecs = executedPlan.collect {
          case a: BaseAggregateExec => a
        }

        val aggregateExecsWithoutPartialAgg = allAggregateExecs.filter {
          _.requiredChildDistribution.head != UnspecifiedDistribution
        }

        // We expect single partial aggregation - remaining agg execs should have child producing
        // expected output partitioning.
        assert(allAggregateExecs.length - 1 === aggregateExecsWithoutPartialAgg.length)

        // For aggregate execs, we make sure output partitioning of the children is same as
        // we expect, HashPartitioning with sub-clustering keys & number of partitions.
        aggregateExecsWithoutPartialAgg.foreach { aggr =>
          assert(requireClusteredDistribution(aggr, Seq(Seq("key1", "key2")),
            Some(numPartitions)))
          assert(hasDesiredHashPartitioningInChildren(aggr, Seq(Seq("key1")),
            numPartitions))
        }
      }
    )
  }
}
