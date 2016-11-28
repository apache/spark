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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

private[spark] object BlockManagerMessages {
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from the master to slaves.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerSlave

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  case class RemoveBlock(private var blockId: BlockId) extends ToBlockManagerSlave
      with KryoSerializable {

    override def write(kryo: Kryo, output: Output): Unit = {
      output.writeString(blockId.name)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      blockId = BlockId(input.readString())
    }
  }

  // Remove all blocks belonging to a specific RDD.
  case class RemoveRdd(private var rddId: Int) extends ToBlockManagerSlave
      with KryoSerializable {

    override def write(kryo: Kryo, output: Output): Unit = {
      output.writeInt(rddId)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      rddId = input.readInt()
    }
  }

  // Remove all blocks belonging to a specific shuffle.
  case class RemoveShuffle(private var shuffleId: Int) extends ToBlockManagerSlave
      with KryoSerializable {

    override def write(kryo: Kryo, output: Output): Unit = {
      output.writeInt(shuffleId)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      shuffleId = input.readInt()
    }
  }

  // Remove all blocks belonging to a specific broadcast.
  case class RemoveBroadcast(private var broadcastId: Long,
      private var removeFromDriver: Boolean = true)
    extends ToBlockManagerSlave with KryoSerializable {

    override def write(kryo: Kryo, output: Output): Unit = {
      output.writeLong(broadcastId)
      output.writeBoolean(removeFromDriver)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      broadcastId = input.readLong()
      removeFromDriver = input.readBoolean()
    }
  }

  /**
   * Driver -> Executor message to trigger a thread dump.
   */
  case object TriggerThreadDump extends ToBlockManagerSlave

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from slaves to the master.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMaster

  case class RegisterBlockManager(
      blockManagerId: BlockManagerId,
      maxMemSize: Long,
      sender: RpcEndpointRef)
    extends ToBlockManagerMaster

  case class UpdateBlockInfo(
      var blockManagerId: BlockManagerId,
      var blockId: BlockId,
      var storageLevel: StorageLevel,
      var memSize: Long,
      var diskSize: Long)
    extends ToBlockManagerMaster
    with Externalizable {

    def this() = this(null, null, null, 0, 0)  // For deserialization only

    override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
      blockManagerId.writeExternal(out)
      out.writeUTF(blockId.name)
      storageLevel.writeExternal(out)
      out.writeLong(memSize)
      out.writeLong(diskSize)
    }

    override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
      blockManagerId = BlockManagerId(in)
      blockId = BlockId(in.readUTF())
      storageLevel = StorageLevel(in)
      memSize = in.readLong()
      diskSize = in.readLong()
    }
  }

  case class GetLocations(blockId: BlockId) extends ToBlockManagerMaster

  case class GetLocationsMultipleBlockIds(blockIds: Array[BlockId]) extends ToBlockManagerMaster

  case class GetPeers(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  case class GetExecutorEndpointRef(executorId: String) extends ToBlockManagerMaster

  case class RemoveExecutor(execId: String) extends ToBlockManagerMaster

  case object StopBlockManagerMaster extends ToBlockManagerMaster

  case object GetMemoryStatus extends ToBlockManagerMaster

  case object GetStorageStatus extends ToBlockManagerMaster

  case class GetBlockStatus(blockId: BlockId, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  case class GetMatchingBlockIds(filter: BlockId => Boolean, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  case class BlockManagerHeartbeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  case class HasCachedBlocks(executorId: String) extends ToBlockManagerMaster
}
