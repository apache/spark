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

package org.apache.spark.shard

import com.google.common.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{EXECUTOR_ID, SHARD_ID, SHARD_MANAGER_ID, SHARD_SET_ID}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shard.ShardManagerMessages._

/**
 * Client-side proxy for the driver's [[ShardManagerMasterEndpoint]].
 *
 * Provides RPC methods for shard set creation, location queries, replica
 * installation, and executor removal used by both driver and executor code.
 */
private[spark] class ShardManagerMaster(
    val masterEndpoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
    extends Logging {

  private val LOCATION_CACHE_MAX_SIZE = 2048

  private val shardLocationsCache = CacheBuilder
    .newBuilder()
    .maximumSize(LOCATION_CACHE_MAX_SIZE)
    .build(new CacheLoader[ShardKey, Seq[ShardManagerId]] {
      override def load(key: ShardKey): Seq[ShardManagerId] = {
        getLocations(key)
      }
    })

  def registerShardManager(
      id: ShardManagerId,
      managerEndpoint: RpcEndpointRef): ShardManagerId = {
    logInfo(log"Registering ShardManager ${MDC(SHARD_MANAGER_ID, id)}")
    val updatedId =
      masterEndpoint.askSync[ShardManagerId](RegisterShardManager(id, managerEndpoint))
    logInfo(log"Registered ShardManager ${MDC(SHARD_MANAGER_ID, updatedId)}")
    updatedId
  }

  def newShardSet(numShards: Int, replicaCount: Int): Long = {
    masterEndpoint.askSync[Long](NewShardSet(numShards, replicaCount))
  }

  def reportShard(shardManagerId: ShardManagerId, setId: Long, id: Int): Unit = {
    masterEndpoint.askSync[Boolean](UpdateShardInfo(shardManagerId, setId, id))
  }

  def installReplicaSet(setId: Long, shardId: Int): Unit = {
    // non-blocking
    masterEndpoint.ask[Boolean](InstallReplicaSet(setId, shardId))
    logInfo(log"Install replica set of shard" +
      log" (${MDC(SHARD_SET_ID, setId)}, ${MDC(SHARD_ID, shardId)}) requested")
  }

  def getLocations(setId: Long, shardId: Int, refresh: Boolean = false): Seq[ShardManagerId] = {
    val key = ShardKey(setId, shardId)
    if (refresh) {
      shardLocationsCache.invalidate(key)
    }
    shardLocationsCache.get(key)
  }

  def removeExecutor(execId: String): Unit = {
    masterEndpoint.ask[Boolean](RemoveExecutor(execId))
    logInfo(log"Removal(shard) of executor ${MDC(EXECUTOR_ID, execId)} requested")
  }

  private def getLocations(key: ShardKey): Seq[ShardManagerId] = {
    masterEndpoint.askSync[Seq[ShardManagerId]](GetLocations(key.setId, key.shardId))
  }

  def stop(): Unit = {
    if (masterEndpoint != null && isDriver) {
      tell(StopShardManagerMaster)
      logInfo(log"ShardManagerMaster stopped")
    }
  }

  private def tell(message: Any): Unit = {
    if (!masterEndpoint.askSync[Boolean](message)) {
      throw new SparkException("Unexpected ShardManagerMasterEndpoint result error")
    }
  }
}

private[spark] object ShardManagerMaster {
  val DRIVER_ENDPOINT_NAME = "ShardManagerMaster"
}
