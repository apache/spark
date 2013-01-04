package spark.streaming.api.java

import scala.collection.JavaConversions._
import java.util.{List => JList}

import spark.streaming._
import dstream.SparkFlumeEvent
import spark.storage.StorageLevel

class JavaStreamingContext(val ssc: StreamingContext) {
  def this(master: String, frameworkName: String, batchDuration: Time) =
    this(new StreamingContext(master, frameworkName, batchDuration))

  def textFileStream(directory: String): JavaDStream[String] = {
    ssc.textFileStream(directory)
  }

  def networkTextStream(hostname: String, port: Int): JavaDStream[String] = {
    ssc.networkTextStream(hostname, port)
  }

  def flumeStream(hostname: String, port: Int, storageLevel: StorageLevel):
    JavaDStream[SparkFlumeEvent] = {
    ssc.flumeStream(hostname, port, storageLevel)
  }

  def start() = ssc.start()
  def stop() = ssc.stop()

}
