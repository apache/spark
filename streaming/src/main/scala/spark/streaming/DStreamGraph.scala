package spark.streaming

import dstream.InputDStream
import java.io.{ObjectInputStream, IOException, ObjectOutputStream}
import collection.mutable.ArrayBuffer
import spark.Logging

final private[streaming] class DStreamGraph extends Serializable with Logging {
  initLogging()

  private val inputStreams = new ArrayBuffer[InputDStream[_]]()
  private val outputStreams = new ArrayBuffer[DStream[_]]()

  private[streaming] var zeroTime: Time = null
  private[streaming] var batchDuration: Duration = null
  private[streaming] var rememberDuration: Duration = null
  private[streaming] var checkpointInProgress = false

  private[streaming] def start(time: Time) {
    this.synchronized {
      if (zeroTime != null) {
        throw new Exception("DStream graph computation already started")
      }
      zeroTime = time
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validate)
      inputStreams.par.foreach(_.start())
    }
  }

  private[streaming] def stop() {
    this.synchronized {
      inputStreams.par.foreach(_.stop())
    }
  }

  private[streaming] def setContext(ssc: StreamingContext) {
    this.synchronized {
      outputStreams.foreach(_.setContext(ssc))
    }
  }

  private[streaming] def setBatchDuration(duration: Duration) {
    this.synchronized {
      if (batchDuration != null) {
        throw new Exception("Batch duration already set as " + batchDuration +
          ". cannot set it again.")
      }
    }
    batchDuration = duration
  }

  private[streaming] def remember(duration: Duration) {
    this.synchronized {
      if (rememberDuration != null) {
        throw new Exception("Batch duration already set as " + batchDuration +
          ". cannot set it again.")
      }
    }
    rememberDuration = duration
  }

  private[streaming] def addInputStream(inputStream: InputDStream[_]) {
    this.synchronized {
      inputStream.setGraph(this)
      inputStreams += inputStream
    }
  }

  private[streaming] def addOutputStream(outputStream: DStream[_]) {
    this.synchronized {
      outputStream.setGraph(this)
      outputStreams += outputStream
    }
  }

  private[streaming] def getInputStreams() = this.synchronized { inputStreams.toArray }

  private[streaming] def getOutputStreams() = this.synchronized { outputStreams.toArray }

  private[streaming] def generateRDDs(time: Time): Seq[Job] = {
    this.synchronized {
      logInfo("Generating RDDs for time " + time)
      outputStreams.flatMap(outputStream => outputStream.generateJob(time))
    }
  }

  private[streaming] def forgetOldRDDs(time: Time) {
    this.synchronized {
      logInfo("Forgetting old RDDs for time " + time)
      outputStreams.foreach(_.forgetOldMetadata(time))
    }
  }

  private[streaming] def updateCheckpointData(time: Time) {
    this.synchronized {
      outputStreams.foreach(_.updateCheckpointData(time))
    }
  }

  private[streaming] def restoreCheckpointData() {
    this.synchronized {
      outputStreams.foreach(_.restoreCheckpointData())
    }
  }

  private[streaming] def validate() {
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

