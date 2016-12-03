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

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import scala.collection.mutable

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.TaskState.TaskState
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{ExecutorLossReason, TaskData, TaskDescription}
import org.apache.spark.util.{SerializableBuffer, Utils}

private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessages {

  case object RetrieveSparkProps extends CoarseGrainedClusterMessage

  case object RetrieveLastAllocatedExecutorId extends CoarseGrainedClusterMessage

  // Driver to executors
  case class LaunchTask(private var task: TaskDescription)
      extends CoarseGrainedClusterMessage with KryoSerializable {

    override def write(kryo: Kryo, output: Output): Unit = {
      task.write(kryo, output)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      task = new TaskDescription(0L, 0, null, null, 0, null)
      task.read(kryo, input)
    }
  }

  case class LaunchTasks(private var tasks: mutable.ArrayBuffer[TaskDescription],
      private var taskDataList: mutable.ArrayBuffer[TaskData])
      extends CoarseGrainedClusterMessage with KryoSerializable {

    override def write(kryo: Kryo, output: Output): Unit = Utils.tryOrIOException {
      val tasks = this.tasks
      val numTasks = tasks.length
      output.writeVarInt(numTasks, true)
      var i = 0
      while (i < numTasks) {
        tasks(i).write(kryo, output)
        i += 1
      }
      val taskDataList = this.taskDataList
      val numData = taskDataList.length
      output.writeVarInt(numData, true)
      i = 0
      while (i < numData) {
        TaskData.write(taskDataList(i), output)
        i += 1
      }
    }

    override def read(kryo: Kryo, input: Input): Unit = Utils.tryOrIOException {
      var numTasks = input.readVarInt(true)
      val tasks = new mutable.ArrayBuffer[TaskDescription](numTasks)
      while (numTasks > 0) {
        val task = new TaskDescription(0, 0, null, null, 0, null)
        task.read(kryo, input)
        tasks += task
        numTasks -= 1
      }
      var numData = input.readVarInt(true)
      val taskDataList = new mutable.ArrayBuffer[TaskData](numData)
      while (numData > 0) {
        taskDataList += TaskData.read(input)
        numData -= 1
      }
      this.tasks = tasks
      this.taskDataList = taskDataList
    }
  }

  case class KillTask(taskId: Long, executor: String, interruptThread: Boolean)
    extends CoarseGrainedClusterMessage

  sealed trait RegisterExecutorResponse

  case object RegisteredExecutor extends CoarseGrainedClusterMessage with RegisterExecutorResponse

  case class RegisterExecutorFailed(message: String) extends CoarseGrainedClusterMessage
    with RegisterExecutorResponse

  // Executors to driver
  case class RegisterExecutor(
      executorId: String,
      executorRef: RpcEndpointRef,
      hostname: String,
      cores: Int,
      logUrls: Map[String, String])
    extends CoarseGrainedClusterMessage

  case class StatusUpdate(var executorId: String, var taskId: Long,
      var state: TaskState, var data: SerializableBuffer)
      extends CoarseGrainedClusterMessage with KryoSerializable {

    override def write(kryo: Kryo, output: Output): Unit = {
      output.writeString(executorId)
      output.writeLong(taskId)
      output.writeVarInt(state.id, true)
      val buffer = data.buffer
      output.writeInt(buffer.remaining())
      Utils.writeByteBuffer(buffer, output)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      executorId = input.readString()
      taskId = input.readLong()
      state = org.apache.spark.TaskState(input.readVarInt(true))
      val len = input.readInt()
      data = new SerializableBuffer(ByteBuffer.wrap(input.readBytes(len)))
    }
  }

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer)
      : StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
    }
  }

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessage

  case object StopDriver extends CoarseGrainedClusterMessage

  case object StopExecutor extends CoarseGrainedClusterMessage

  case object StopExecutors extends CoarseGrainedClusterMessage

  case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
    extends CoarseGrainedClusterMessage

  case class SetupDriver(driver: RpcEndpointRef) extends CoarseGrainedClusterMessage

  // Exchanged between the driver and the AM in Yarn client mode
  case class AddWebUIFilter(
      filterName: String, filterParams: Map[String, String], proxyBase: String)
    extends CoarseGrainedClusterMessage

  // Messages exchanged between the driver and the cluster manager for executor allocation
  // In Yarn mode, these are exchanged between the driver and the AM

  case class RegisterClusterManager(am: RpcEndpointRef) extends CoarseGrainedClusterMessage

  // Request executors by specifying the new total number of executors desired
  // This includes executors already pending or running
  case class RequestExecutors(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int])
    extends CoarseGrainedClusterMessage

  // Check if an executor was force-killed but for a reason unrelated to the running tasks.
  // This could be the case if the executor is preempted, for instance.
  case class GetExecutorLossReason(executorId: String) extends CoarseGrainedClusterMessage

  case class KillExecutors(executorIds: Seq[String]) extends CoarseGrainedClusterMessage

  // Used internally by executors to shut themselves down.
  case object Shutdown extends CoarseGrainedClusterMessage

}
