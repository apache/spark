package spark.streaming

import java.io.{ObjectInputStream, IOException, ObjectOutputStream}
import collection.mutable.ArrayBuffer
import spark.Logging

final class DStreamGraph extends Serializable with Logging {
  initLogging()

  private val inputStreams = new ArrayBuffer[InputDStream[_]]()
  private val outputStreams = new ArrayBuffer[DStream[_]]()

  private[streaming] var zeroTime: Time = null
  private[streaming] var checkpointInProgress = false;

  def start(time: Time) {
    this.synchronized {
      if (zeroTime != null) {
        throw new Exception("DStream graph computation already started")
      }
      zeroTime = time
      outputStreams.foreach(_.initialize(zeroTime))
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

  def addInputStream(inputStream: InputDStream[_]) {
    inputStream.setGraph(this)
    inputStreams += inputStream
  }

  def addOutputStream(outputStream: DStream[_]) {
    outputStream.setGraph(this)
    outputStreams += outputStream
  }

  def getInputStreams() = inputStreams.toArray

  def getOutputStreams() = outputStreams.toArray

  def generateRDDs(time: Time): Seq[Job] = {
    this.synchronized {
      outputStreams.flatMap(outputStream => outputStream.generateJob(time))
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

