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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.util.Utils

final private[streaming] class DStreamGraph extends Serializable with Logging {

  private val inputStreams = new ArrayBuffer[InputDStream[_]]()
  private val outputStreams = new ArrayBuffer[DStream[_]]()

  var rememberDuration: Duration = null
  var checkpointInProgress = false

  var zeroTime: Time = null
  var startTime: Time = null
  var batchDuration: Duration = null

  def start(time: Time) {
    this.synchronized {
      require(zeroTime == null, "DStream graph computation already started")
      zeroTime = time
      startTime = time
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validateAtStart)
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
      require(batchDuration == null,
        s"Batch duration already set as $batchDuration. Cannot set it again.")
      batchDuration = duration
    }
  }

  def remember(duration: Duration) {
    this.synchronized {
      require(rememberDuration == null,
        s"Remember duration already set as $rememberDuration. Cannot set it again.")
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

  def getInputStreams(): Array[InputDStream[_]] = this.synchronized { inputStreams.toArray }

  def getOutputStreams(): Array[DStream[_]] = this.synchronized { outputStreams.toArray }

  def getReceiverInputStreams(): Array[ReceiverInputDStream[_]] = this.synchronized {
    inputStreams.filter(_.isInstanceOf[ReceiverInputDStream[_]])
      .map(_.asInstanceOf[ReceiverInputDStream[_]])
      .toArray
  }

  def getInputStreamName(streamId: Int): Option[String] = synchronized {
    inputStreams.find(_.id == streamId).map(_.name)
  }

  def generateJobs(time: Time): Seq[Job] = {
    logDebug("Generating jobs for time " + time)
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }
    }
    logDebug("Generated " + jobs.length + " jobs for time " + time)
    jobs
  }

  def clearMetadata(time: Time) {
    logDebug("Clearing metadata for time " + time)
    this.synchronized {
      outputStreams.foreach(_.clearMetadata(time))
    }
    logDebug("Cleared old metadata for time " + time)
  }

  def updateCheckpointData(time: Time) {
    logInfo("Updating checkpoint data for time " + time)
    this.synchronized {
      outputStreams.foreach(_.updateCheckpointData(time))
    }
    logInfo("Updated checkpoint data for time " + time)
  }

  def clearCheckpointData(time: Time) {
    logInfo("Clearing checkpoint data for time " + time)
    this.synchronized {
      outputStreams.foreach(_.clearCheckpointData(time))
    }
    logInfo("Cleared checkpoint data for time " + time)
  }

  def restoreCheckpointData() {
    logInfo("Restoring checkpoint data")
    this.synchronized {
      outputStreams.foreach(_.restoreCheckpointData())
    }
    logInfo("Restored checkpoint data")
  }

  def validate() {
    this.synchronized {
      require(batchDuration != null, "Batch duration has not been set")
      // assert(batchDuration >= Milliseconds(100), "Batch duration of " + batchDuration +
      // " is very low")
      require(getOutputStreams().nonEmpty, "No output operations registered, so nothing to execute")
    }
  }

  /**
   * Get the maximum remember duration across all the input streams. This is a conservative but
   * safe remember duration which can be used to perform cleanup operations.
   */
  def getMaxInputStreamRememberDuration(): Duration = {
    // If an InputDStream is not used, its `rememberDuration` will be null and we can ignore them
    inputStreams.map(_.rememberDuration).filter(_ != null).maxBy(_.milliseconds)
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    logDebug("DStreamGraph.writeObject used")
    this.synchronized {
      checkpointInProgress = true
      logDebug("Enabled checkpoint mode")
      oos.defaultWriteObject()
      checkpointInProgress = false
      logDebug("Disabled checkpoint mode")
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug("DStreamGraph.readObject used")
    this.synchronized {
      checkpointInProgress = true
      ois.defaultReadObject()
      checkpointInProgress = false
    }
  }
}

