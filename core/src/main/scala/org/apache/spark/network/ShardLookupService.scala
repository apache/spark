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

package org.apache.spark.network

import scala.concurrent.{Future, Promise}

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shard.{ShardLookupListener, ShardStoreClient}
import org.apache.spark.shard.ShardManager

private[spark] abstract class ShardLookupService extends ShardStoreClient {

  def init(shardManager: ShardManager): Unit

  def port: Int

  def hostName: String

  /**
   * Install a shard replica on this executor. Called when a remote executor
   * requests this executor to host a copy of a shard.
   *
   * The default reads the shard data from BlockManager (Java-serialized
   * HashedRelation). Override in custom implementations (e.g., native
   * backends) to load data in a different format.
   *
   * @return true if the replica was installed successfully
   */
  def onInstallReplica(shardManager: ShardManager, setId: Long, shardId: Int): Unit = {
    shardManager.loadShardData(setId, shardId)
  }

  def fetchBatch(host: String, port: Int, reqMsg: ManagedBuffer): Future[ManagedBuffer] = {
    val result = Promise[ManagedBuffer]()
    fetchBatch(
      host,
      port,
      reqMsg,
      new ShardLookupListener {

        override def onBatchFetchSuccess(response: ManagedBuffer): Unit = {
          result.success(response)
        }

        override def onBatchFetchFailure(exception: Throwable): Unit = {
          result.failure(exception)
        }
      })
    result.future
  }

}
