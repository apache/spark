package spark.streaming

import spark.RDD
import spark.Logging
import spark.SparkEnv
import spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

class SparkStreamContext (
    master: String,
    frameworkName: String,
    val sparkHome: String = null,
    val jars: Seq[String] = Nil)
  extends Logging {
  
  initLogging()

  val sc = new SparkContext(master, frameworkName, sparkHome, jars)
  val env = SparkEnv.get
  
  val inputRDSs = new ArrayBuffer[InputRDS[_]]()
  val outputRDSs = new ArrayBuffer[RDS[_]]()
  var batchDuration: Time = null 
  var scheduler: Scheduler = null
  
  def setBatchDuration(duration: Long) {
    setBatchDuration(Time(duration))
  }
  
  def setBatchDuration(duration: Time) {
    batchDuration = duration
  }
    
  /*
  def createNetworkStream[T: ClassManifest](
      name: String,
      addresses: Array[InetSocketAddress],
      batchDuration: Time): RDS[T] = {
    
    val inputRDS = new NetworkInputRDS[T](this, addresses)
    inputRDSs += inputRDS
    inputRDS
  }  
  
  def createNetworkStream[T: ClassManifest](
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
  */

  /** 
   * This function creates a input stream that monitors a Hadoop-compatible
   * for new files and executes the necessary processing on them.
   */  
  def createFileStream[
    K: ClassManifest, 
    V: ClassManifest, 
    F <: NewInputFormat[K, V]: ClassManifest
  ](directory: String): RDS[(K, V)] = {
    val inputRDS = new FileInputRDS[K, V, F](this, new Path(directory))
    inputRDSs += inputRDS
    inputRDS
  }

  def createTextFileStream(directory: String): RDS[String] = {
    createFileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
  }
  
  /**
   * This function create a input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue 
   */
  def createQueueStream[T: ClassManifest](
      queue: Queue[RDD[T]],      
      oneAtATime: Boolean = true,
      defaultRDD: RDD[T] = null
    ): RDS[T] = {
    val inputRDS = new QueueInputRDS(this, queue, oneAtATime, defaultRDD)
    inputRDSs += inputRDS
    inputRDS
  }
  
  def createQueueStream[T: ClassManifest](iterator: Iterator[RDD[T]]): RDS[T] = {
    val queue = new Queue[RDD[T]]
    val inputRDS = createQueueStream(queue, true, null)
    queue ++= iterator
    inputRDS
  } 

  
  /**
   * This function registers a RDS as an output stream that will be 
   * computed every interval.
   */  
  def registerOutputStream (outputRDS: RDS[_]) {
    outputRDSs += outputRDS
  }
  
  /**
   * This function verify whether the stream computation is eligible to be executed.
   */
  def verify() {
    if (batchDuration == null) {
      throw new Exception("Batch duration has not been set")
    }
    if (batchDuration < Milliseconds(100)) {
      logWarning("Batch duration of " + batchDuration + " is very low")
    }
    if (inputRDSs.size == 0) {
      throw new Exception("No input RDSes created, so nothing to take input from")
    }
    if (outputRDSs.size == 0) {
      throw new Exception("No output RDSes registered, so nothing to execute")      
    }
    
  }
  
  /**
   * This function starts the execution of the streams. 
   */  
  def start() {
    verify()
    scheduler = new Scheduler(this, inputRDSs.toArray, outputRDSs.toArray)
    scheduler.start()
  }
  
  /**
   * This function starts the execution of the streams. 
   */
  def stop() {
    try {
      scheduler.stop()
      sc.stop() 
    } catch {
      case e: Exception => logWarning("Error while stopping", e)
    }
    
    logInfo("SparkStreamContext stopped")
  }
}


object SparkStreamContext {
  implicit def rdsToPairRdsFunctions [K: ClassManifest, V: ClassManifest] (rds: RDS[(K,V)]) = 
    new PairRDSFunctions (rds)
}
