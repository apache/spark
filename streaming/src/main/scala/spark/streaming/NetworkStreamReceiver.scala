package spark.streaming

import spark.Logging
import spark.storage.StorageLevel

import scala.math._
import scala.collection.mutable.{Queue, HashMap, ArrayBuffer}
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

import java.io.BufferedWriter
import java.io.OutputStreamWriter

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

/*import akka.actor.Actor._*/

class NetworkStreamReceiver[T: ClassManifest] (
    inputName: String, 
    intervalDuration: Time, 
    splitId: Int, 
    ssc: SparkStreamContext,
    tempDirectory: String) 
  extends DaemonActor
  with Logging {

  /**
   * Assume all data coming in has non-decreasing timestamp. 
   */
  final class Inbox[T: ClassManifest] (intervalDuration: Time) {
    var currentBucket: (Interval, ArrayBuffer[T]) = null
    val filledBuckets = new Queue[(Interval, ArrayBuffer[T])]()

    def += (tuple: (Time, T)) = addTuple(tuple)

    def addTuple(tuple: (Time, T)) {
      val (time, data) = tuple
      val interval = getInterval (time)

      filledBuckets.synchronized {
        if (currentBucket == null) {
          currentBucket = (interval, new ArrayBuffer[T]())
        }

        if (interval != currentBucket._1) {
          filledBuckets += currentBucket
          currentBucket = (interval, new ArrayBuffer[T]())
        }

        currentBucket._2 += data
      }
    }

    def getInterval(time: Time): Interval = {
      val intervalBegin = time.floor(intervalDuration) 
      Interval (intervalBegin, intervalBegin + intervalDuration) 
    }

    def hasFilledBuckets(): Boolean = {
      filledBuckets.synchronized {
        return filledBuckets.size > 0
      }
    }

    def popFilledBucket(): (Interval, ArrayBuffer[T]) = {
      filledBuckets.synchronized {
        if (filledBuckets.size == 0) {
          return null
        }
        return filledBuckets.dequeue()
      }
    }
  }
  
  val inbox = new Inbox[T](intervalDuration)
  lazy val sparkstreamScheduler = {
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt 
    val url = "akka://spark@%s:%s/user/SparkStreamScheduler".format(host, port)
    ssc.actorSystem.actorFor(url)
  }
  /*sparkstreamScheduler ! Test()*/
  
  val intervalDurationMillis = intervalDuration.asInstanceOf[LongTime].milliseconds
  val useBlockManager = true 

  initLogging()

  override def act() {
    // register the InputReceiver 
    val port = 7078
    RemoteActor.alive(port)
    RemoteActor.register(Symbol("NetworkStreamReceiver-"+inputName), self)
    logInfo("Registered actor on port " + port)
   
    loop {
      reactWithin (getSleepTime) {
        case TIMEOUT => 
          flushInbox() 
        case data =>
          val t = data.asInstanceOf[T]
          inbox += (getTimeFromData(t), t)
      }
    }
  }

  def getSleepTime(): Long = {
    (System.currentTimeMillis / intervalDurationMillis + 1) *
        intervalDurationMillis - System.currentTimeMillis 
  }

  def getTimeFromData(data: T): Time = {
    LongTime(System.currentTimeMillis)
  }

  def flushInbox()  {
    while (inbox.hasFilledBuckets) {
      inbox.synchronized {
        val (interval, data) = inbox.popFilledBucket()
        val dataArray = data.toArray
        logInfo("Received " + dataArray.length + " items at interval " + interval)
        val reference = {
          if (useBlockManager) {
            writeToBlockManager(dataArray, interval)
          } else {
            writeToDisk(dataArray, interval)
          }
        }
        if (reference != null) {
          logInfo("Notifying scheduler")
          sparkstreamScheduler ! InputGenerated(inputName, interval, reference.toString)
        }
      }
    }
  }

  def writeToDisk(data: Array[T], interval: Interval): String = {
    try {
      // TODO(Haoyuan): For current test, the following writing to file lines could be
      // commented.
      val fs = new Path(tempDirectory).getFileSystem(new Configuration())
      val inputDir = new Path(
        tempDirectory, 
        inputName + "-" + interval.toFormattedString)
      val inputFile = new Path(inputDir, "part-" + splitId)
      logInfo("Writing to file " + inputFile)
      if (System.getProperty("spark.fake", "false") != "true") {
        val writer = new BufferedWriter(new OutputStreamWriter(fs.create(inputFile, true)))
        data.foreach(x => writer.write(x.toString + "\n"))
        writer.close()
      } else {
        logInfo("Fake file")
      }
      inputFile.toString 
    }catch {
      case e: Exception => 
      logError("Exception writing to file at interval " + interval + ": " + e.getMessage, e)
      null
    }
  }

  def writeToBlockManager(data: Array[T], interval: Interval): String = {
    try{
      val blockId = inputName + "-" + interval.toFormattedString + "-" + splitId
      if (System.getProperty("spark.fake", "false") != "true") {
        logInfo("Writing as block " + blockId )
        ssc.env.blockManager.put(blockId.toString, data.toIterator, StorageLevel.DISK_AND_MEMORY)
      } else {
        logInfo("Fake block")
      }
      blockId
    } catch {
      case e: Exception => 
      logError("Exception writing to block manager at interval " + interval + ": " + e.getMessage, e)
      null
    }
  }
}
