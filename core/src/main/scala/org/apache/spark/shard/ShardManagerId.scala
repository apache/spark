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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import com.google.common.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.util.Utils

private[spark] class ShardManagerId(
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int,
    private var topologyInfo_ : Option[String])
    extends Externalizable {

  private def this() = this(null, null, 0, None) // For deserialization only

  def executorId: String = executorId_

  if (null != host_) {
    Utils.checkHost(host_)
    require(port_ > 0, s"port must be positive: $port_")
  }

  def hostPort: String = {
    Utils.checkHost(host)
    require(port > 0, s"port must be positive: $port")
    host + ":" + port
  }

  def host: String = host_

  def port: Int = port_

  def topologyInfo: Option[String] = topologyInfo_

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
    out.writeBoolean(topologyInfo_.isDefined)
    topologyInfo.foreach(out.writeUTF)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
    val isTopologyInfoAvailable = in.readBoolean()
    topologyInfo_ = if (isTopologyInfoAvailable) Option(in.readUTF()) else None
  }

  override def toString: String = s"ShardManagerId($executorId, $host, $port, $topologyInfo)"

  override def hashCode: Int =
    ((executorId.hashCode * 41 + host.hashCode) * 41 + port) * 41 + topologyInfo.hashCode

  override def equals(that: Any): Boolean = that match {
    case id: ShardManagerId =>
      executorId == id.executorId &&
      port == id.port &&
      host == id.host &&
      topologyInfo == id.topologyInfo
    case _ =>
      false
  }
}

private[spark] object ShardManagerId {
  def apply(
      execId: String,
      host: String,
      port: Int,
      topologyInfo: Option[String] = None): ShardManagerId =
    getCachedShardManagerId(new ShardManagerId(execId, host, port, topologyInfo))

  def apply(in: ObjectInput): ShardManagerId = {
    val obj = new ShardManagerId()
    obj.readExternal(in)
    getCachedShardManagerId(obj)
  }

  private val shardManagerIdCache = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .build(new CacheLoader[ShardManagerId, ShardManagerId]() {
      override def load(id: ShardManagerId): ShardManagerId = id
    })

  private def getCachedShardManagerId(id: ShardManagerId): ShardManagerId = {
    shardManagerIdCache.get(id)
  }
}
