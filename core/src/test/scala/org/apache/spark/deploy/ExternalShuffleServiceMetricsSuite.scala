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

package org.apache.spark.deploy

import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.{SHUFFLE_SERVICE_DB_ENABLED, SHUFFLE_SERVICE_ENABLED}
import org.apache.spark.util.Utils

class ExternalShuffleServiceMetricsSuite extends SparkFunSuite {

  var sparkConf: SparkConf = _
  var externalShuffleService: ExternalShuffleService = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkConf = new SparkConf()
    sparkConf.set(SHUFFLE_SERVICE_ENABLED, true)
    sparkConf.set(SHUFFLE_SERVICE_DB_ENABLED, false)
    sparkConf.set("spark.local.dir", System.getProperty("java.io.tmpdir"))
    Utils.loadDefaultSparkProperties(sparkConf, null)
    val securityManager = new SecurityManager(sparkConf)
    externalShuffleService = new ExternalShuffleService(sparkConf, securityManager)
    externalShuffleService.start()
  }

  override def afterAll(): Unit = {
    if (externalShuffleService != null) {
      externalShuffleService.stop()
    }
    super.afterAll()
  }

  test("SPARK-31646: metrics should be registered") {
    val sourceRef = classOf[ExternalShuffleService].getDeclaredField("shuffleServiceSource")
    sourceRef.setAccessible(true)
    val source = sourceRef.get(externalShuffleService).asInstanceOf[ExternalShuffleServiceSource]
    // Use sorted Seq instead of Set for easier comparison when there is a mismatch
    assert(source.metricRegistry.getMetrics.keySet().asScala.toSeq.sorted ==
      Seq(
        "blockTransferRate",
        "blockTransferMessageRate",
        "blockTransferRateBytes",
        "blockTransferAvgSize_1min",
        "numActiveConnections",
        "numCaughtExceptions",
        "numRegisteredConnections",
        "openBlockRequestLatencyMillis",
        "registeredExecutorsSize",
        "registerExecutorRequestLatencyMillis",
        "shuffle-server.usedDirectMemory",
        "shuffle-server.usedHeapMemory",
        "finalizeShuffleMergeLatencyMillis",
        "fetchMergedBlocksMetaLatencyMillis").sorted
    )
  }
}
