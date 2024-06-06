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

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingDeduplicateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StatefulOpClusteredDistributionTestHelper
import org.apache.spark.util.Utils

class StreamingDeduplicationDistributionSuite extends StreamTest
  with StatefulOpClusteredDistributionTestHelper {

  import testImplicits._

  test("SPARK-38204: streaming deduplication should require StatefulOpClusteredDistribution " +
    "from children") {

    val input = MemoryStream[Int]
    val df1 = input.toDF()
      .select($"value" as Symbol("key1"), $"value" * 2 as Symbol("key2"),
        $"value" * 3 as Symbol("value"))
    val dedup = df1.repartition($"key1").dropDuplicates("key1", "key2")

    testStream(dedup, OutputMode.Update())(
      AddData(input, 1, 1, 2, 3, 4),
      CheckAnswer((1, 2, 3), (2, 4, 6), (3, 6, 9), (4, 8, 12)),
      Execute { query =>
        val numPartitions = query.lastExecution.numStateStores

        val dedupExecs = query.lastExecution.executedPlan.collect {
          case d: StreamingDeduplicateExec => d
        }

        assert(dedupExecs.length === 1)
        assert(requireStatefulOpClusteredDistribution(
          dedupExecs.head, Seq(Seq("key1", "key2")), numPartitions))
        assert(hasDesiredHashPartitioningInChildren(
          dedupExecs.head, Seq(Seq("key1", "key2")), numPartitions))
      }
    )
  }

  test("SPARK-38204: streaming deduplication should require ClusteredDistribution " +
    "from children if the query starts from checkpoint in prior to 3.3") {

    val inputData = MemoryStream[Int]
    val df1 = inputData.toDF()
      .select($"value" as Symbol("key1"), $"value" * 2 as Symbol("key2"),
        $"value" * 3 as Symbol("value"))
    val dedup = df1.repartition($"key1").dropDuplicates("key1", "key2")

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-3.2.0-deduplication-with-repartition/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(1, 1, 2)
    inputData.addData(3, 4)

    testStream(dedup, Update)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION.key -> "true")),

      // scalastyle:off line.size.limit
      /*
        Note: The checkpoint was generated using the following input in Spark version 3.2.0
        AddData(inputData, 1, 1, 2),
        CheckLastBatch((1, 2, 3), (2, 4, 6)),
        AddData(inputData, 3, 4),
        CheckLastBatch((3, 6, 9), (4, 8, 12))

        Note2: The following plans are the physical plans of the query in older Spark versions
        The physical plans around StreamingDeduplicate are quite similar, especially shuffles
        being injected are same. That said, verifying with checkpoint being built with
        Spark 3.2.0 would verify the following versions as well.

        A. Spark 3.2.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@76467fb2, org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$1900/1334867523@32b72162
        +- StreamingDeduplicate [key1#3, key2#4], state info [ checkpoint = file:/blabla/state, runId = bf82c05e-4031-4421-89e0-28fd9127eb5b, opId = 0, ver = 1, numPartitions = 5], 0
           +- Exchange hashpartitioning(key1#3, 5), REPARTITION_BY_COL, [id=#115]
              +- *(1) Project [value#1 AS key1#3, (value#1 * 2) AS key2#4, (value#1 * 3) AS value#5]
                 +- MicroBatchScan[value#1] MemoryStreamDataSource

        B. Spark 3.1.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@133d8337
        +- StreamingDeduplicate [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-c0b73191-75ec-4a54-89b7-368fbbc4b2a8/state, runId = 9b2baaee-1147-4faf-98b4-3c3d8ee34966, opId = 0, ver = 1, numPartitions = 5], 0
           +- Exchange hashpartitioning(key1#3, 5), REPARTITION, [id=#117]
              +- *(1) Project [value#1 AS key1#3, (value#1 * 2) AS key2#4, (value#1 * 3) AS value#5]
                 +- *(1) Project [value#1]
                    +- MicroBatchScan[value#1] MemoryStreamDataSource

        C. Spark 3.0.0
        WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@bb06c00
        +- StreamingDeduplicate [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-6f8a96c7-2af5-4952-a1b4-c779766334ef/state, runId = 9a208eb0-d915-46dd-a0fd-23b1df82b951, opId = 0, ver = 1, numPartitions = 5], 0
           +- Exchange hashpartitioning(key1#3, 5), false, [id=#57]
              +- *(1) Project [value#1 AS key1#3, (value#1 * 2) AS key2#4, (value#1 * 3) AS value#5]
                 +- *(1) Project [value#1]
                    +- MicroBatchScan[value#1] MemoryStreamDataSource

        D. Spark 2.4.0
        StreamingDeduplicate [key1#3, key2#4], state info [ checkpoint = file:/tmp/spark-d8a684a0-5623-4739-85e8-e45b99768aa7/state, runId = 85bd75bd-3d45-4d42-aeac-9e45fc559ee9, opId = 0, ver = 1, numPartitions = 5], 0
        +- Exchange hashpartitioning(key1#3, 5)
           +- *(1) Project [value#37 AS key1#3, (value#37 * 2) AS key2#4, (value#37 * 3) AS value#5]
              +- *(1) Project [value#37]
                 +- *(1) ScanV2 MemoryStreamDataSource$[value#37]
       */
      // scalastyle:on line.size.limit

      AddData(inputData, 2, 3, 4, 5),
      CheckLastBatch((5, 10, 15)),
      Execute { query =>
        val executedPlan = query.lastExecution.executedPlan
        assert(!executedPlan.conf.getConf(SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION))

        val numPartitions = query.lastExecution.numStateStores

        val dedupExecs = executedPlan.collect {
          case d: StreamingDeduplicateExec => d
        }

        assert(dedupExecs.length === 1)
        assert(requireClusteredDistribution(
          dedupExecs.head, Seq(Seq("key1", "key2")), Some(numPartitions)))
        assert(hasDesiredHashPartitioningInChildren(
          dedupExecs.head, Seq(Seq("key1")), numPartitions))
      }
    )
  }
}
