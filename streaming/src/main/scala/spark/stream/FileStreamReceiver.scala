package spark.stream

import spark.Logging

import scala.collection.mutable.HashSet
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

class FileStreamReceiver (
    inputName: String,
    rootDirectory: String,
    intervalDuration: Long)
  extends Logging {

  val pollInterval = 100
  val sparkstreamScheduler = {
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt + 1
    RemoteActor.select(Node(host, port), 'SparkStreamScheduler)
  }
  val directory = new Path(rootDirectory)
  val fs = directory.getFileSystem(new Configuration())
  val files = new HashSet[String]()
  var time: Long = 0

  def start() {
    fs.mkdirs(directory)
    files ++= getFiles() 

    actor {
      logInfo("Monitoring directory - " + rootDirectory)
      while(true) {
        testFiles(getFiles())
        Thread.sleep(pollInterval)
      }
    }
  }

  def getFiles(): Iterable[String] = {
    fs.listStatus(directory).map(_.getPath.toString)
  }

  def testFiles(fileList: Iterable[String]) {
    fileList.foreach(file => {
      if (!files.contains(file)) {
        if (!file.endsWith("_tmp")) {
          notifyFile(file)
        }
        files += file
      }
    })
  }

  def notifyFile(file: String) {
    logInfo("Notifying file " + file)
    time += intervalDuration
    val interval  = Interval(LongTime(time), LongTime(time + intervalDuration))
    sparkstreamScheduler ! InputGenerated(inputName, interval, file)
  }
}
  

