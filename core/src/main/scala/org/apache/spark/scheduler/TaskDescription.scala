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

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.collection.immutable
import scala.collection.mutable.{HashMap, Map}
import scala.jdk.CollectionConverters._

import org.apache.spark.{JobArtifactSet, JobArtifactState}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

/**
 * Description of a task that gets passed onto executors to be executed, usually created by
 * `TaskSetManager.resourceOffer`.
 *
 * TaskDescriptions and the associated Task need to be serialized carefully for two reasons:
 *
 *     (1) When a TaskDescription is received by an Executor, the Executor needs to first get the
 *         list of JARs and files and add these to the classpath, and set the properties, before
 *         deserializing the Task object (serializedTask). This is why the Properties are included
 *         in the TaskDescription, even though they're also in the serialized task.
 *     (2) Because a TaskDescription is serialized and sent to an executor for each task, efficient
 *         serialization (both in terms of serialization time and serialized buffer size) is
 *         important. For this reason, we serialize TaskDescriptions ourselves with the
 *         TaskDescription.encode and TaskDescription.decode methods.  This results in a smaller
 *         serialized size because it avoids serializing unnecessary fields in the Map objects
 *         (which can introduce significant overhead when the maps are small).
 */
private[spark] class TaskDescription(
    val taskId: Long,
    val attemptNumber: Int,
    val executorId: String,
    val name: String,
    val index: Int,    // Index within this task's TaskSet
    val partitionId: Int,
    val artifacts: JobArtifactSet,
    val properties: Properties,
    val cpus: Int,
    // resources is the total resources assigned to the task
    // Eg, Map("gpu" -> Map("0" -> ResourceAmountUtils.toInternalResource(0.7))):
    // assign 0.7 of the gpu address "0" to this task
    val resources: immutable.Map[String, immutable.Map[String, Long]],
    val serializedTask: ByteBuffer) {

  assert(cpus > 0, "CPUs per task should be > 0")

  override def toString: String = s"TaskDescription($name)"
}

private[spark] object TaskDescription {
  private def serializeStringLongMap(map: Map[String, Long], dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(map.size)
    map.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      dataOut.writeLong(value)
    }
  }

  private def serializeResources(map: immutable.Map[String, immutable.Map[String, Long]],
      dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(map.size)
    map.foreach { case (rName, addressAmountMap) =>
      dataOut.writeUTF(rName)
      dataOut.writeInt(addressAmountMap.size)
      addressAmountMap.foreach { case (address, amount) =>
        dataOut.writeUTF(address)
        dataOut.writeLong(amount)
      }
    }
  }

  def encode(taskDescription: TaskDescription): ByteBuffer = {
    val bytesOut = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(bytesOut)

    dataOut.writeLong(taskDescription.taskId)
    dataOut.writeInt(taskDescription.attemptNumber)
    dataOut.writeUTF(taskDescription.executorId)
    dataOut.writeUTF(taskDescription.name)
    dataOut.writeInt(taskDescription.index)
    dataOut.writeInt(taskDescription.partitionId)

    // Write artifacts
    serializeArtifacts(taskDescription.artifacts, dataOut)

    // Write properties.
    dataOut.writeInt(taskDescription.properties.size())
    taskDescription.properties.asScala.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      // SPARK-19796 -- writeUTF doesn't work for long strings, which can happen for property values
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      dataOut.writeInt(bytes.length)
      dataOut.write(bytes)
    }

    // Write cpus.
    dataOut.writeInt(taskDescription.cpus)

    // Write resources.
    serializeResources(taskDescription.resources, dataOut)

    // Write the task. The task is already serialized, so write it directly to the byte buffer.
    Utils.writeByteBuffer(taskDescription.serializedTask, bytesOut)

    dataOut.close()
    bytesOut.close()
    bytesOut.toByteBuffer
  }

  private def deserializeOptionString(in: DataInputStream): Option[String] = {
    if (in.readBoolean()) {
      Some(in.readUTF())
    } else {
      None
    }
  }

  private def deserializeArtifacts(dataIn: DataInputStream): JobArtifactSet = {
    new JobArtifactSet(
      state = deserializeOptionString(dataIn).map { uuid =>
        JobArtifactState(
          uuid = uuid,
          replClassDirUri = deserializeOptionString(dataIn))
      },
      jars = immutable.Map(deserializeStringLongMap(dataIn).toSeq: _*),
      files = immutable.Map(deserializeStringLongMap(dataIn).toSeq: _*),
      archives = immutable.Map(deserializeStringLongMap(dataIn).toSeq: _*))
  }

  private def serializeOptionString(str: Option[String], out: DataOutputStream): Unit = {
    out.writeBoolean(str.isDefined)
    if (str.isDefined) {
      out.writeUTF(str.get)
    }
  }

  private def serializeArtifacts(artifacts: JobArtifactSet, dataOut: DataOutputStream): Unit = {
    serializeOptionString(artifacts.state.map(_.uuid), dataOut)
    artifacts.state.foreach { state =>
      serializeOptionString(state.replClassDirUri, dataOut)
    }
    serializeStringLongMap(Map(artifacts.jars.toSeq: _*), dataOut)
    serializeStringLongMap(Map(artifacts.files.toSeq: _*), dataOut)
    serializeStringLongMap(Map(artifacts.archives.toSeq: _*), dataOut)
  }

  private def deserializeStringLongMap(dataIn: DataInputStream): HashMap[String, Long] = {
    val map = new HashMap[String, Long]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      map(dataIn.readUTF()) = dataIn.readLong()
      i += 1
    }
    map
  }

  private def deserializeResources(dataIn: DataInputStream):
      immutable.Map[String, immutable.Map[String, Long]] = {
    val map = new HashMap[String, immutable.Map[String, Long]]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      val resType = dataIn.readUTF()
      val addressAmountMap = new HashMap[String, Long]()
      val addressAmountSize = dataIn.readInt()
      var j = 0
      while (j < addressAmountSize) {
        val address = dataIn.readUTF()
        val amount = dataIn.readLong()
        addressAmountMap(address) = amount
        j += 1
      }
      map.put(resType, addressAmountMap.toMap)
      i += 1
    }
    map.toMap
  }

  def decode(byteBuffer: ByteBuffer): TaskDescription = {
    val dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer))
    val taskId = dataIn.readLong()
    val attemptNumber = dataIn.readInt()
    val executorId = dataIn.readUTF()
    val name = dataIn.readUTF()
    val index = dataIn.readInt()
    val partitionId = dataIn.readInt()

    // Read artifacts.
    val artifacts = deserializeArtifacts(dataIn)

    // Read properties.
    val properties = new Properties()
    val numProperties = dataIn.readInt()
    for (i <- 0 until numProperties) {
      val key = dataIn.readUTF()
      val valueLength = dataIn.readInt()
      val valueBytes = new Array[Byte](valueLength)
      dataIn.readFully(valueBytes)
      properties.setProperty(key, new String(valueBytes, StandardCharsets.UTF_8))
    }

    // Read cpus.
    val cpus = dataIn.readInt()

    // Read resources.
    val resources = deserializeResources(dataIn)

    // Create a sub-buffer for the serialized task into its own buffer (to be deserialized later).
    val serializedTask = byteBuffer.slice()

    new TaskDescription(taskId, attemptNumber, executorId, name, index, partitionId, artifacts,
      properties, cpus, resources, serializedTask)
  }
}
