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
import org.apache.spark.util.SerializableBuffer

private[spark] class TaskDescription(
    val taskId: Long,
    val executorId: String,
    val name: String,
    val index: Int,    // Index within this task's TaskSet
    _serializedTask: ByteBuffer)
  extends Serializable {

  // Because ByteBuffers are not serializable, wrap the task in a SerializableBuffer
  private val buffer = new SerializableBuffer(_serializedTask)

  def serializedTask: ByteBuffer = buffer.value

  override def toString: String = "TaskDescription(TID=%d, index=%d)".format(taskId, index)
}
