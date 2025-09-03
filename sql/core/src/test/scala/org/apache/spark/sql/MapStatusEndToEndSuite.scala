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
import org.apache.spark.internal.config.SHUFFLE_ORDER_INDEPENDENT_CHECKSUM_ENABLED
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class MapStatusEndToEndSuite extends SparkFunSuite with SQLTestUtils {
    override def spark: SparkSession = SparkSession.builder()
      .master("local")
      .config(SHUFFLE_ORDER_INDEPENDENT_CHECKSUM_ENABLED.key, value = true)
      .config(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key, value = 5)
      .getOrCreate()

  override def afterAll(): Unit = {
    // This suite should not interfere with the other test suites.
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearDefaultSession()
  }

  ignore("Propagate checksum from executor to driver") {
    assert(spark.sparkContext.conf.get("spark.shuffle.orderIndependentChecksum.enabled") == "true")
    assert(spark.conf.get("spark.shuffle.orderIndependentChecksum.enabled") == "true")
    assert(spark.sparkContext.conf.get("spark.sql.leafNodeDefaultParallelism") == "5")
    assert(spark.conf.get("spark.sql.leafNodeDefaultParallelism") == "5")

    withTable("t") {
      spark.range(1000).repartition(10).write.mode("overwrite").
        saveAsTable("t")
    }

    val shuffleStatuses = spark.sparkContext.env.mapOutputTracker.
      asInstanceOf[MapOutputTrackerMaster].shuffleStatuses
    assert(shuffleStatuses.size == 1)

    val mapStatuses = shuffleStatuses(0).mapStatuses
    assert(mapStatuses.length == 5)
    assert(mapStatuses.forall(_.checksumValue != 0))
  }
}
