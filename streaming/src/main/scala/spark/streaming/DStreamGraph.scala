package spark.streaming

import java.io.{ObjectInputStream, IOException, ObjectOutputStream}
import collection.mutable.ArrayBuffer
import spark.Logging

final class DStreamGraph extends Serializable with Logging {
  initLogging()

  private val inputStreams = new ArrayBuffer[InputDStream[_]]()
  private val outputStreams = new ArrayBuffer[DStream[_]]()

  private[streaming] var zeroTime: Time = null
  private[streaming] var batchDuration: Time = null
  private[streaming] var rememberDuration: Time = null
  private[streaming] var checkpointInProgress = false

  def start(time: Time) {
    this.synchronized {
      if (zeroTime != null) {
        throw new Exception("DStream graph computation already started")
      }
      zeroTime = time
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.setRememberDuration(rememberDuration))
      outputStreams.foreach(_.validate)
      inputStreams.par.foreach(_.start())
    }
  }

  def stop() {
    this.synchronized {
      inputStreams.par.foreach(_.stop())
    }
  }

  private[streaming] def setContext(ssc: StreamingContext) {
    this.synchronized {
      outputStreams.foreach(_.setContext(ssc))
    }
  }

  def setBatchDuration(duration: Time) {
    this.synchronized {
      if (batchDuration != null) {
        throw new Exception("Batch duration already set as " + batchDuration +
          ". cannot set it again.")
      }
    }
    batchDuration = duration
  }

  def setRememberDuration(duration: Time) {
    this.synchronized {
      if (rememberDuration != null) {
        throw new Exception("Batch duration already set as " + batchDuration +
          ". cannot set it again.")
      }
    }
    rememberDuration = duration
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

  def getInputStreams() = inputStreams.toArray

  def getOutputStreams() = outputStreams.toArray

  def generateRDDs(time: Time): Seq[Job] = {
    this.synchronized {
      outputStreams.flatMap(outputStream => outputStream.generateJob(time))
    }
  }

  def forgetOldRDDs(time: Time) {
    this.synchronized {
      outputStreams.foreach(_.forgetOldRDDs(time))
    }
  }

  def validate() {
    this.synchronized {
      assert(batchDuration != null, "Batch duration has not been set")
      assert(batchDuration > Milliseconds(100), "Batch duration of " + batchDuration + " is very low")
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

