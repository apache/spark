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

package org.apache.spark.streaming

import dstream.InputDStream
import java.io.{ObjectInputStream, IOException, ObjectOutputStream}
import collection.mutable.ArrayBuffer
import org.apache.spark.Logging

final private[streaming] class DStreamGraph extends Serializable with Logging {
  initLogging()

  private val inputStreams = new ArrayBuffer[InputDStream[_]]()
  private val outputStreams = new ArrayBuffer[DStream[_]]()

  var rememberDuration: Duration = null
  var checkpointInProgress = false

  var zeroTime: Time = null
  var startTime: Time = null
  var batchDuration: Duration = null

  def start(time: Time) {
    this.synchronized {
      if (zeroTime != null) {
        throw new Exception("DStream graph computation already started")
      }
      zeroTime = time
      startTime = time
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validate)
      inputStreams.par.foreach(_.start())
    }
  }

  def restart(time: Time) {
    this.synchronized { startTime = time }
  }

  def stop() {
    this.synchronized {
      inputStreams.par.foreach(_.stop())
    }
  }

  def setContext(ssc: StreamingContext) {
    this.synchronized {
      outputStreams.foreach(_.setContext(ssc))
    }
  }

  def setBatchDuration(duration: Duration) {
    this.synchronized {
      if (batchDuration != null) {
        throw new Exception("Batch duration already set as " + batchDuration +
          ". cannot set it again.")
      }
      batchDuration = duration
    }
  }

  def remember(duration: Duration) {
    this.synchronized {
      if (rememberDuration != null) {
        throw new Exception("Batch duration already set as " + batchDuration +
          ". cannot set it again.")
      }
      rememberDuration = duration
    }
  }

  def addInputStream(inputStream: InputDStream[_]) {
    this.synchronized {
      inputStream.setGraph(this)
      inputStreams += inputStream
    }
  }

  def addOutputStream(outputStream: DStream[_]) {
    this.synchronized {
      outputStream.setGraph(this)
      outputStreams += outputStream
    }
  }

  def getInputStreams() = this.synchronized { inputStreams.toArray }

  def getOutputStreams() = this.synchronized { outputStreams.toArray }

  def generateJobs(time: Time): Seq[Job] = {
    this.synchronized {
      logInfo("Generating jobs for time " + time)
      val jobs = outputStreams.flatMap(outputStream => outputStream.generateJob(time))
      logInfo("Generated " + jobs.length + " jobs for time " + time)
      jobs
    }
  }

  def clearOldMetadata(time: Time) {
    this.synchronized {
      logInfo("Clearing old metadata for time " + time)
      outputStreams.foreach(_.clearOldMetadata(time))
      logInfo("Cleared old metadata for time " + time)
    }
  }

  def updateCheckpointData(time: Time) {
    this.synchronized {
      logInfo("Updating checkpoint data for time " + time)
      outputStreams.foreach(_.updateCheckpointData(time))
      logInfo("Updated checkpoint data for time " + time)
    }
  }

  def restoreCheckpointData() {
    this.synchronized {
      logInfo("Restoring checkpoint data")
      outputStreams.foreach(_.restoreCheckpointData())
      logInfo("Restored checkpoint data")
    }
  }

  def validate() {
    this.synchronized {
      assert(batchDuration != null, "Batch duration has not been set")
      //assert(batchDuration >= Milliseconds(100), "Batch duration of " + batchDuration + " is very low")
      assert(getOutputStreams().size > 0, "No output streams registered, so nothing to execute")
    }
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    this.synchronized {
      logDebug("DStreamGraph.writeObject used")
      checkpointInProgress = true
      oos.defaultWriteObject()
      checkpointInProgress = false
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream) {
    this.synchronized {
      logDebug("DStreamGraph.readObject used")
      checkpointInProgress = true
      ois.defaultReadObject()
      checkpointInProgress = false
    }
  }
}

