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

import scala.collection.mutable
import scala.collection.parallel.immutable.ParVector

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.util.Utils

final private[streaming] class DStreamGraph extends Serializable with Logging {

  private var inputStreams = mutable.ArraySeq.empty[InputDStream[_]]
  private var outputStreams = mutable.ArraySeq.empty[DStream[_]]

  @volatile private var inputStreamNameAndID: Seq[(String, Int)] = Nil

  var rememberDuration: Duration = null
  var checkpointInProgress = false

  var zeroTime: Time = null
  var startTime: Time = null
  var batchDuration: Duration = null
  @volatile private var numReceivers: Int = 0

  def start(time: Time): Unit = {
    this.synchronized {
      require(zeroTime == null, "DStream graph computation already started")
      zeroTime = time
      startTime = time
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validateAtStart())
      numReceivers = inputStreams.count(_.isInstanceOf[ReceiverInputDStream[_]])
      inputStreamNameAndID = inputStreams.map(is => (is.name, is.id)).toSeq
      // scalastyle:off parvector
      new ParVector(inputStreams.toVector).foreach(_.start())
      // scalastyle:on parvector
    }
  }

  def restart(time: Time): Unit = {
    this.synchronized { startTime = time }
  }

  def stop(): Unit = {
    this.synchronized {
      // scalastyle:off parvector
      new ParVector(inputStreams.toVector).foreach(_.stop())
      // scalastyle:on parvector
    }
  }

  def setContext(ssc: StreamingContext): Unit = {
    this.synchronized {
      outputStreams.foreach(_.setContext(ssc))
    }
  }

  def setBatchDuration(duration: Duration): Unit = {
    this.synchronized {
      require(batchDuration == null,
        s"Batch duration already set as $batchDuration. Cannot set it again.")
      batchDuration = duration
    }
  }

  def remember(duration: Duration): Unit = {
    this.synchronized {
      require(rememberDuration == null,
        s"Remember duration already set as $rememberDuration. Cannot set it again.")
      rememberDuration = duration
    }
  }

  def addInputStream(inputStream: InputDStream[_]): Unit = {
    this.synchronized {
      inputStream.setGraph(this)
      inputStreams = inputStreams :+ inputStream
    }
  }

  def addOutputStream(outputStream: DStream[_]): Unit = {
    this.synchronized {
      outputStream.setGraph(this)
      outputStreams = outputStreams :+ outputStream
    }
  }

  def getInputStreams(): Array[InputDStream[_]] = this.synchronized { inputStreams.toArray }

  def getOutputStreams(): Array[DStream[_]] = this.synchronized { outputStreams.toArray }

  def getReceiverInputStreams(): Array[ReceiverInputDStream[_]] = this.synchronized {
    inputStreams.filter(_.isInstanceOf[ReceiverInputDStream[_]])
      .map(_.asInstanceOf[ReceiverInputDStream[_]])
      .toArray
  }

  def getNumReceivers: Int = numReceivers

  def getInputStreamNameAndID: Seq[(String, Int)] = inputStreamNameAndID

  def generateJobs(time: Time): Seq[Job] = {
    logDebug("Generating jobs for time " + time)
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }.toSeq
    }
    logDebug("Generated " + jobs.length + " jobs for time " + time)
    jobs
  }

  def clearMetadata(time: Time): Unit = {
    logDebug("Clearing metadata for time " + time)
    this.synchronized {
      outputStreams.foreach(_.clearMetadata(time))
    }
    logDebug("Cleared old metadata for time " + time)
  }

  def updateCheckpointData(time: Time): Unit = {
    logInfo(log"Updating checkpoint data for time ${MDC(LogKeys.TIME, time)}")
    this.synchronized {
      outputStreams.foreach(_.updateCheckpointData(time))
    }
    logInfo(log"Updated checkpoint data for time ${MDC(LogKeys.TIME, time)}")
  }

  def clearCheckpointData(time: Time): Unit = {
    logInfo(log"Clearing checkpoint data for time ${MDC(LogKeys.TIME, time)}")
    this.synchronized {
      outputStreams.foreach(_.clearCheckpointData(time))
    }
    logInfo(log"Cleared checkpoint data for time ${MDC(LogKeys.TIME, time)}")
  }

  def restoreCheckpointData(): Unit = {
    logInfo("Restoring checkpoint data")
    this.synchronized {
      outputStreams.foreach(_.restoreCheckpointData())
    }
    logInfo("Restored checkpoint data")
  }

  def validate(): Unit = {
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

