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

package org.apache.spark.storage

import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef}
import org.apache.spark.util.ResetSystemProperties

class BlockTTLIntegrationSuite extends SparkFunSuite with LocalSparkContext
    with ResetSystemProperties with Eventually {

  val numExecs = 3
  val numParts = 3
  val TaskStarted = "TASK_STARTED"
  val TaskEnded = "TASK_ENDED"
  val JobEnded = "JOB_ENDED"

  // TODO(holden): This is shared with MapOutputTrackerSuite move to a BlockTestUtils or similar.
  private def fetchDeclaredField(value: AnyRef, fieldName: String): AnyRef = {
    val field = value.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(value)
  }

  private def lookupBlockManagerMasterEndpoint(sc: SparkContext): BlockManagerMasterEndpoint = {
    val rpcEnv = sc.env.rpcEnv
    val dispatcher = fetchDeclaredField(rpcEnv, "dispatcher")
    fetchDeclaredField(dispatcher, "endpointRefs").
      asInstanceOf[java.util.Map[RpcEndpoint, RpcEndpointRef]].asScala.
      filter(_._1.isInstanceOf[BlockManagerMasterEndpoint]).
      head._1.asInstanceOf[BlockManagerMasterEndpoint]
  }

  test(s"Test that shuffle blocks are recorded.") {
    val conf = new SparkConf()
      .setAppName("test-blockmanager-decommissioner")
      .setMaster("local-cluster[2, 1, 1024]")
      .set(config.SPARK_TTL_SHUFFLE_BLOCK_CLEANER, 100L)
    sc = new SparkContext(conf)
    sc.setLogLevel("DEBUG")
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    assert(managerMasterEndpoint.blockAccessTime.isEmpty)
    // Trigger a shuffle
    sc.parallelize(1.to(100)).groupBy(_ % 10).count()
    // Check that the blocks were registered with the TTL tracker
    assert(!managerMasterEndpoint.blockAccessTime.isEmpty)
  }
}
