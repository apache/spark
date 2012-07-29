package spark.stream

import spark.SparkContext
import spark.SparkEnv
import spark.Utils
import spark.Logging

import scala.collection.mutable.ArrayBuffer

import java.net.InetSocketAddress
import java.io.IOException
import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import akka.actor._
import akka.actor.Actor
import akka.util.duration._

class SparkStreamContext (
    master: String,
    frameworkName: String,
    val sparkHome: String = null,
    val jars: Seq[String] = Nil)
  extends Logging {

  initLogging()

  val sc = new SparkContext(master, frameworkName, sparkHome, jars)
  val env = SparkEnv.get
  val actorSystem = env.actorSystem

  @transient val inputRDSs = new ArrayBuffer[InputRDS[_]]()
  @transient val outputRDSs = new ArrayBuffer[RDS[_]]()
  
  var tempDirRoot: String = null
  var tempDir: Path = null

  def readNetworkStream[T: ClassManifest](
      name: String,
      addresses: Array[InetSocketAddress],
      batchDuration: Time): RDS[T] = {
    
    val inputRDS = new NetworkInputRDS[T](name, addresses, batchDuration, this)
    inputRDSs += inputRDS
    inputRDS
  }
  
  def readNetworkStream[T: ClassManifest](
      name: String,
      addresses: Array[String],
      batchDuration: Long): RDS[T] = {
    
    def stringToInetSocketAddress (str: String): InetSocketAddress = {
      val parts = str.split(":")
      if (parts.length != 2) {
        throw new IllegalArgumentException ("Address format error")
      }
      new InetSocketAddress(parts(0), parts(1).toInt)
    }

    readNetworkStream(
        name,
        addresses.map(stringToInetSocketAddress).toArray,
        LongTime(batchDuration))
  }

  def readFileStream(name: String, directory: String): RDS[String] = {
    val path = new Path(directory)
    val fs = path.getFileSystem(new Configuration())
    val qualPath = path.makeQualified(fs)
    val inputRDS = new FileInputRDS(name, qualPath.toString, this)
    inputRDSs += inputRDS
    inputRDS
  }

  def readTestStream(name: String, batchDuration: Long): RDS[String] = {
    val inputRDS = new TestInputRDS(name, LongTime(batchDuration), this)
    inputRDSs += inputRDS 
    inputRDS
  }

  def registerOutputStream (outputRDS: RDS[_]) {
    outputRDSs += outputRDS
  }

  def setTempDir(dir: String) {
    tempDirRoot = dir
  }

  def run () {
    val ctxt = this
    val actor = actorSystem.actorOf(
        Props(new Scheduler(ctxt,  inputRDSs.toArray, outputRDSs.toArray)),
        name = "SparkStreamScheduler")
    logInfo("Registered actor")
    actorSystem.awaitTermination()
  }
}

object SparkStreamContext {
  implicit def rdsToPairRdsFunctions [K: ClassManifest, V: ClassManifest] (rds: RDS[(K,V)]) = 
    new PairRDSFunctions (rds)
}
