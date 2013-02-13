package spark.streaming

import dstream.InputDStream
import java.io.{ObjectInputStream, IOException, ObjectOutputStream}
import collection.mutable.ArrayBuffer
import spark.Logging

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
    }
    batchDuration = duration
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

  def generateRDDs(time: Time): Seq[Job] = {
    this.synchronized {
      logInfo("Generating RDDs for time " + time)
      outputStreams.flatMap(outputStream => outputStream.generateJob(time))
    }
  }

  def clearOldMetadata(time: Time) {
    this.synchronized {
      logInfo("Clearing old metadata for time " + time)
      outputStreams.foreach(_.clearOldMetadata(time))
    }
  }

  def updateCheckpointData(time: Time) {
    this.synchronized {
      outputStreams.foreach(_.updateCheckpointData(time))
    }
  }

  def restoreCheckpointData() {
    this.synchronized {
      outputStreams.foreach(_.restoreCheckpointData())
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

