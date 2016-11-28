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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.util.{SerializableBuffer, Utils}

/**
 * Description of a task that gets passed onto executors to be executed, usually created by
 * [[TaskSetManager.resourceOffer]].
 */
private[spark] class TaskDescription(
    private var _taskId: Long,
    private var _attemptNumber: Int,
    private var _executorId: String,
    private var _name: String,
    private var _index: Int,    // Index within this task's TaskSet
    @transient private var _serializedTask: ByteBuffer)
  extends Serializable with KryoSerializable {

  def taskId: Long = _taskId
  def attemptNumber: Int = _attemptNumber
  def executorId: String = _executorId
  def name: String = _name
  def index: Int = _index

  // Because ByteBuffers are not serializable, wrap the task in a SerializableBuffer
  private val buffer =
    if (_serializedTask ne null) new SerializableBuffer(_serializedTask) else null

  def serializedTask: ByteBuffer =
    if (_serializedTask ne null) _serializedTask else buffer.value

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeLong(_taskId)
    output.writeVarInt(_attemptNumber, true)
    output.writeString(_executorId)
    output.writeString(_name)
    output.writeInt(_index)
    output.writeInt(_serializedTask.remaining())
    Utils.writeByteBuffer(_serializedTask, output)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    _taskId = input.readLong()
    _attemptNumber = input.readVarInt(true)
    _executorId = input.readString()
    _name = input.readString()
    _index = input.readInt()
    val len = input.readInt()
    _serializedTask = ByteBuffer.wrap(input.readBytes(len))
  }

  override def toString: String = "TaskDescription(TID=%d, index=%d)".format(taskId, index)
}
