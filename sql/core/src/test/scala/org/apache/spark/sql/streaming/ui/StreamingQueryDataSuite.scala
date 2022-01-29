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

package org.apache.spark.sql.streaming.ui

import java.util.UUID

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.status.{ElementTrackingStore, KVUtils}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.InMemoryStore

class StreamingQueryDataSuite extends SparkFunSuite {
  test("SPARK-38056: test writing StreamingQueryData to an in-memory store") {
    val store = new ElementTrackingStore(new InMemoryStore(), new SparkConf())
    store.write(testStreamingQueryData)
  }

  test("SPARK-38056: test writing StreamingQueryData to a LevelDB store") {
    val testDir = Utils.createTempDir()
    try {
      val kvStore = KVUtils.open(testDir, getClass.getName)
      val store = new ElementTrackingStore(kvStore, new SparkConf())
      store.write(testStreamingQueryData)
    } finally {
      Utils.deleteRecursively(testDir)
    }
  }

  private def testStreamingQueryData: StreamingQueryData = {
    val id = UUID.randomUUID()
    new StreamingQueryData(
      "some-query",
      id,
      id.toString,
      isActive = false,
      None,
      1L,
      None
    )
  }
}
