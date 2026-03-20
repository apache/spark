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

import org.apache.spark.sql.execution.datasources.v2.state.{StateSourceOptions, StreamStreamJoinTestUtils}
import org.apache.spark.sql.internal.SQLConf

/**
 * Integration test suite for stream-stream join operator repartitioning.
 */
class OfflineStateRepartitionJoinCkptV1IntegrationSuite
  extends OfflineStateRepartitionIntegrationSuiteBase {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, "1")
  }

  // Test stream-stream join operator repartitioning
  Seq(1, 2, 3).foreach { version =>
    testWithAllRepartitionOperations(s"stream-stream join ver $version") { newPartitions =>
      withSQLConf(SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString) {
        val allStoreNames = StreamStreamJoinTestUtils.allStoreNames
        val storeToOptions = if (version <= 2) {
          allStoreNames.map { storeName =>
            storeName -> Map(StateStore.DEFAULT_COL_FAMILY_NAME -> Map.empty[String, String])
          }.toMap
        } else {
          Map(StateStoreId.DEFAULT_STORE_NAME -> allStoreNames.map { colFamilyName =>
            colFamilyName -> Map(StateSourceOptions.STORE_NAME -> colFamilyName)
          }.toMap)
        }

        testRepartitionWorkflow[(Int, Long)](
          newPartitions = newPartitions,
          setupInitialState = (inputData, checkpointDir, _) => {
            val query = getStreamStreamJoinQuery(inputData)
            testStream(query)(
              StartStream(checkpointLocation = checkpointDir),
              // Batch 1: Creates state in all 4 column families
              AddData(inputData, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
              CheckNewAnswer((2, 2, 2, 2), (4, 4, 4, 4)),
              // Batch 2: Adds more state to all column families
              AddData(inputData, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
              CheckNewAnswer((6, 6, 6, 6), (8, 8, 8, 8), (10, 10, 10, 10)),
              StopStream
            )
          },
          verifyResumedQuery = (inputData, checkpointDir, _) => {
            val query = getStreamStreamJoinQuery(inputData)
            testStream(query)(
              StartStream(checkpointLocation = checkpointDir),
              AddData(inputData, (6, 10L), (8, 9L)),
              CheckNewAnswer((6, 10, 6, 10), (6, 6, 6, 10), (8, 9, 8, 9), (8, 8, 8, 9))
            )
          },
          storeToColumnFamilyToStateSourceOptions = storeToOptions
        )
      }
    }
  }
}

class OfflineStateRepartitionJoinCkptV2IntegrationSuite
  extends OfflineStateRepartitionJoinCkptV1IntegrationSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, "2")
  }
}
