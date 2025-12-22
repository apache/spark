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

package org.apache.spark.sql

import org.apache.spark.{MapOutputTrackerMaster, SparkFunSuite}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class MapStatusEndToEndSuite extends SparkFunSuite with SQLTestUtils {
    override def spark: SparkSession = SparkSession.builder()
      .master("local")
      .config(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key, value = 5)
      .config(SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key, value = false)
      .getOrCreate()

  override def afterAll(): Unit = {
    // This suite should not interfere with the other test suites.
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearDefaultSession()
  }

  test("Propagate checksum from executor to driver") {
    assert(spark.sparkContext.conf.get(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key) == "5")
    assert(spark.conf.get(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key) == "5")
    assert(spark.sparkContext.conf.get(SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key)
      == "false")
    assert(spark.conf.get(SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key) == "false")

    var shuffleId = 0
    Seq(("true", "false"), ("false", "true"), ("true", "true")).foreach {
      case (orderIndependentChecksumEnabled: String, checksumMismatchFullRetryEnabled: String) =>
        withSQLConf(
          SQLConf.SHUFFLE_ORDER_INDEPENDENT_CHECKSUM_ENABLED.key ->
            orderIndependentChecksumEnabled,
          SQLConf.SHUFFLE_CHECKSUM_MISMATCH_FULL_RETRY_ENABLED.key ->
            checksumMismatchFullRetryEnabled) {
          withTable("t") {
            spark.range(1000).repartition(10).write.mode("overwrite").
              saveAsTable("t")
          }

          val shuffleStatuses = spark.sparkContext.env.mapOutputTracker.
            asInstanceOf[MapOutputTrackerMaster].shuffleStatuses
          assert(shuffleStatuses.contains(shuffleId))

          val mapStatuses = shuffleStatuses(shuffleId).mapStatuses
          assert(mapStatuses.length == 5)
          assert(mapStatuses.forall(_.checksumValue != 0))
          shuffleId += 1
        }
    }
  }
}
