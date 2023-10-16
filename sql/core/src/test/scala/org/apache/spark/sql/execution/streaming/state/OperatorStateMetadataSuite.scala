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

package org.apache.spark.sql.execution.streaming.state

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, session_window}
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class OperatorStateMetadataSuite extends StreamTest with SharedSparkSession {
  import testImplicits._

  private lazy val hadoopConf = spark.sessionState.newHadoopConf()

  private def numShufflePartitions = spark.sessionState.conf.numShufflePartitions

  private def sameOperatorStateMetadata(
                                         operatorMetadata1: OperatorStateMetadataV1,
                                         operatorMetadata2: OperatorStateMetadataV1
                                       ): Boolean = {
    operatorMetadata1.operatorInfo == operatorMetadata2.operatorInfo &&
      operatorMetadata1.stateStoreInfo.deep == operatorMetadata2.stateStoreInfo.deep
  }

  test("Serialize and deserialize stateful operator metadata") {
    val stateDir = Utils.createTempDir()
    val statePath = new Path(stateDir.getCanonicalPath)
    val stateStoreInfo = (1 to 4).map(i => StateStoreMetadataV1(s"store$i", 1, 200))
    val operatorInfo = OperatorInfoV1(1, "Join")
    val operatorMetadata = OperatorStateMetadataV1(operatorInfo, stateStoreInfo.toArray)
    new OperatorStateMetadataWriter(statePath, hadoopConf).write(operatorMetadata)
    val operatorMetadata1 = new OperatorStateMetadataReader(statePath, hadoopConf).read()
      .asInstanceOf[OperatorStateMetadataV1]
    assert(sameOperatorStateMetadata(operatorMetadata, operatorMetadata1))
  }

  test("Stateful operator metadata for streaming aggregation") {
    val inputData = MemoryStream[Int]
    val checkpointDir = Utils.createTempDir().getAbsolutePath
    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    testStream(aggregated, Complete)(
      StartStream(checkpointLocation = checkpointDir),
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      StopStream
    )

    val statePath = new Path(checkpointDir, "state/0/")
    val operatorMetadata = new OperatorStateMetadataReader(statePath, hadoopConf).read()
      .asInstanceOf[OperatorStateMetadataV1]
    val expectedMetadata = OperatorStateMetadataV1(OperatorInfoV1(0, "stateStoreSave"),
      Array(StateStoreMetadataV1("default", 0, numShufflePartitions)))
    assert(sameOperatorStateMetadata(operatorMetadata, expectedMetadata))
  }

  test("Stateful operator metadata for streaming join") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF.select($"value" as "key", ($"value" * 2) as "leftValue")
    val df2 = input2.toDF.select($"value" as "key", ($"value" * 3) as "rightValue")
    val joined = df1.join(df2, "key")

    val checkpointDir = Utils.createTempDir().getAbsolutePath
    testStream(joined)(
      StartStream(checkpointLocation = checkpointDir),
      AddData(input1, 1),
      CheckAnswer(),
      AddData(input2, 1, 10),       // 1 arrived on input1 first, then input2, should join
      CheckNewAnswer((1, 2, 3)),
      StopStream
    )

    val statePath = new Path(checkpointDir, "state/0/")
    val operatorMetadata = new OperatorStateMetadataReader(statePath, hadoopConf).read()
      .asInstanceOf[OperatorStateMetadataV1]

    val expectedStateStoreInfo = Array(
      StateStoreMetadataV1("left-keyToNumValues", 0, numShufflePartitions),
      StateStoreMetadataV1("left-keyWithIndexToValue", 0, numShufflePartitions),
      StateStoreMetadataV1("right-keyToNumValues", 0, numShufflePartitions),
      StateStoreMetadataV1("right-keyWithIndexToValue", 0, numShufflePartitions))

    val expectedMetadata = OperatorStateMetadataV1(
      OperatorInfoV1(0, "symmetricHashJoin"), expectedStateStoreInfo)
    assert(sameOperatorStateMetadata(operatorMetadata, expectedMetadata))
  }

  test("Stateful operator metadata for streaming session window") {
    val input = MemoryStream[(String, Long)]
    val sessionWindow: Column = session_window($"eventTime", "10 seconds")

    val checkpointDir = Utils.createTempDir().getAbsolutePath

    val events = input.toDF()
      .select($"_1".as("value"), $"_2".as("timestamp"))
      .withColumn("eventTime", $"timestamp".cast("timestamp"))
      .withWatermark("eventTime", "30 seconds")
      .selectExpr("explode(split(value, ' ')) AS sessionId", "eventTime")

    val streamingDf = events
      .groupBy(sessionWindow as Symbol("session"), $"sessionId")
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
        "numEvents")

    testStream(streamingDf, OutputMode.Complete())(
      StartStream(checkpointLocation = checkpointDir),
      AddData(input,
        ("hello world spark streaming", 40L),
        ("world hello structured streaming", 41L)
      ),
      CheckNewAnswer(
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("streaming", 40, 51, 11, 2),
        ("spark", 40, 50, 10, 1),
        ("structured", 41, 51, 10, 1)
      ),
      StopStream
    )

    val statePath = new Path(checkpointDir, "state/0/")
    val operatorMetadata = new OperatorStateMetadataReader(statePath, hadoopConf).read()
      .asInstanceOf[OperatorStateMetadataV1]

    val expectedMetadata = OperatorStateMetadataV1(
      OperatorInfoV1(0, "sessionWindowStateStoreSaveExec"),
      Array(StateStoreMetadataV1("default", 1, spark.sessionState.conf.numShufflePartitions))
    )
    assert(sameOperatorStateMetadata(operatorMetadata, expectedMetadata))
  }
}
