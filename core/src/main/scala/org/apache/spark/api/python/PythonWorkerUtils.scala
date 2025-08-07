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

package org.apache.spark.api.python

import java.io.{DataInputStream, DataOutputStream, File}
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkEnv, SparkFiles}
import org.apache.spark.api.python.PythonFunction.PythonAccumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging

private[spark] object PythonWorkerUtils extends Logging {

  /**
   * Write a string in UTF-8 charset.
   *
   * It will be read by `UTF8Deserializer.loads` in Python.
   */
  def writeUTF(str: String, dataOut: DataOutputStream): Unit = {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    writeBytes(bytes, dataOut)
  }

  /**
   * Write a byte array.
   *
   * It will be read by `FramedSerializer._read_with_length` in Python.
   */
  def writeBytes(bytes: Array[Byte], dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  /**
   * Write a Python version to check if the Python version is expected.
   *
   * It will be read and checked by `worker_util.check_python_version`.
   */
  def writePythonVersion(pythonVer: String, dataOut: DataOutputStream): Unit = {
    writeUTF(pythonVer, dataOut)
  }

  /**
   * Write Spark files to set up them in the worker.
   *
   * It will be read and used by `worker_util.setup_spark_files`.
   */
  def writeSparkFiles(
      jobArtifactUUID: Option[String],
      pythonIncludes: Set[String],
      dataOut: DataOutputStream): Unit = {
    // sparkFilesDir
    val root = jobArtifactUUID.map { uuid =>
      new File(SparkFiles.getRootDirectory(), uuid).getAbsolutePath
    }.getOrElse(SparkFiles.getRootDirectory())
    writeUTF(root, dataOut)

    // Python includes (*.zip and *.egg files)
    dataOut.writeInt(pythonIncludes.size)
    for (include <- pythonIncludes) {
      writeUTF(include, dataOut)
    }
  }

  /**
   * Write broadcasted variables to set up them in the worker.
   *
   * It will be read and used by 'worker_util.setup_broadcasts`.
   */
  def writeBroadcasts(
      broadcastVars: Seq[Broadcast[PythonBroadcast]],
      worker: PythonWorker,
      env: SparkEnv,
      dataOut: DataOutputStream): Unit = {
    // Broadcast variables
    val oldBids = PythonRDD.getWorkerBroadcasts(worker)
    val newBids = broadcastVars.map(_.id).toSet
    // number of different broadcasts
    val toRemove = oldBids.diff(newBids)
    val addedBids = newBids.diff(oldBids)
    val cnt = toRemove.size + addedBids.size
    val needsDecryptionServer = env.serializerManager.encryptionEnabled && addedBids.nonEmpty
    dataOut.writeBoolean(needsDecryptionServer)
    dataOut.writeInt(cnt)
    def sendBidsToRemove(): Unit = {
      for (bid <- toRemove) {
        // remove the broadcast from worker
        dataOut.writeLong(-bid - 1) // bid >= 0
        oldBids.remove(bid)
      }
    }
    if (needsDecryptionServer) {
      // if there is encryption, we setup a server which reads the encrypted files, and sends
      // the decrypted data to python
      val idsAndFiles = broadcastVars.flatMap { broadcast =>
        if (!oldBids.contains(broadcast.id)) {
          oldBids.add(broadcast.id)
          Some((broadcast.id, broadcast.value.path))
        } else {
          None
        }
      }
      val server = new EncryptedPythonBroadcastServer(env, idsAndFiles)
      server.connInfo match {
        case portNum: Int =>
          dataOut.writeInt(portNum)
          writeUTF(server.secret, dataOut)
        case sockPath: String =>
          dataOut.writeInt(-1)
          writeUTF(sockPath, dataOut)
      }
      logTrace(s"broadcast decryption server setup on ${server.connInfo}")
      sendBidsToRemove()
      idsAndFiles.foreach { case (id, _) =>
        // send new broadcast
        dataOut.writeLong(id)
      }
      dataOut.flush()
    } else {
      sendBidsToRemove()
      for (broadcast <- broadcastVars) {
        if (!oldBids.contains(broadcast.id)) {
          // send new broadcast
          dataOut.writeLong(broadcast.id)
          writeUTF(broadcast.value.path, dataOut)
          oldBids.add(broadcast.id)
        }
      }
    }
    dataOut.flush()
  }

  /**
   * Write PythonFunction to the worker.
   */
  def writePythonFunction(func: PythonFunction, dataOut: DataOutputStream): Unit = {
    writeBytes(func.command.toArray, dataOut)
  }

  /**
   * Read a string in UTF-8 charset.
   */
  def readUTF(dataIn: DataInputStream): String = {
    readUTF(dataIn.readInt(), dataIn)
  }

  /**
   * Read a string in UTF-8 charset with the given byte length.
   */
  def readUTF(length: Int, dataIn: DataInputStream): String = {
    new String(readBytes(length, dataIn), StandardCharsets.UTF_8)
  }

  /**
   * Read a byte array.
   */
  def readBytes(dataIn: DataInputStream): Array[Byte] = {
    readBytes(dataIn.readInt(), dataIn)
  }

  /**
   * Read a byte array with the given byte length.
   */
  def readBytes(length: Int, dataIn: DataInputStream): Array[Byte] = {
    if (length == 0) {
      Array.emptyByteArray
    } else {
      val obj = new Array[Byte](length)
      dataIn.readFully(obj)
      obj
    }
  }

  /**
   * Receive accumulator updates from the worker.
   *
   * The updates are sent by `worker_util.send_accumulator_updates`.
   */
  def receiveAccumulatorUpdates(
      maybeAccumulator: Option[PythonAccumulator],
      dataIn: DataInputStream): Unit = {
    val numAccumulatorUpdates = dataIn.readInt()
    (1 to numAccumulatorUpdates).foreach { _ =>
      val update = readBytes(dataIn)
      maybeAccumulator.foreach(_.add(update))
    }
  }
}
