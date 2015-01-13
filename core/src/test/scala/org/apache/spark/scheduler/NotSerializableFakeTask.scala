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

import java.io.{ObjectInputStream, ObjectOutputStream, IOException}

import org.apache.spark.TaskContext

/**
 * A Task implementation that fails to serialize.
 */
private[spark] class NotSerializableFakeTask(myId: Int, stageId: Int) extends Task[Array[Byte]](stageId, 0) {
  override def runTask(context: TaskContext): Array[Byte] = Array.empty[Byte]
  override def preferredLocations: Seq[TaskLocation] = Seq[TaskLocation]()

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    if (stageId == 0) {
      throw new IllegalStateException("Cannot serialize")
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {}
}
