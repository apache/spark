package spark.streaming

import spark.streaming.util.RecurringTimer
import spark.streaming.util.Clock
import spark.SparkEnv
import spark.Logging

import scala.collection.mutable.HashMap


sealed trait SchedulerMessage
case class InputGenerated(inputName: String, interval: Interval, reference: AnyRef = null) extends SchedulerMessage

class Scheduler(
    ssc: StreamingContext, 
    inputStreams: Array[InputDStream[_]],
    outputStreams: Array[DStream[_]])
extends Logging {

  initLogging()

  val concurrentJobs = System.getProperty("spark.stream.concurrentJobs", "1").toInt
  val jobManager = new JobManager(ssc, concurrentJobs)
  val clockClass = System.getProperty("spark.streaming.clock", "spark.streaming.util.SystemClock")
  val clock = Class.forName(clockClass).newInstance().asInstanceOf[Clock]
  val timer = new RecurringTimer(clock, ssc.batchDuration, generateRDDs(_))
  
  def start() {
    
    val zeroTime = Time(timer.start())
    outputStreams.foreach(_.initialize(zeroTime))
    inputStreams.par.foreach(_.start())
    logInfo("Scheduler started")
  }
  
  def stop() {
    timer.stop()
    inputStreams.par.foreach(_.stop())
    logInfo("Scheduler stopped")    
  }
  
  def generateRDDs (time: Time) {
    logInfo("\n-----------------------------------------------------\n")
    logInfo("Generating RDDs for time " + time)
    outputStreams.foreach(outputStream => {
        outputStream.generateJob(time) match {
          case Some(job) => submitJob(job)
          case None => 
        }
      }
    )
    logInfo("Generated RDDs for time " + time)
  }

  def submitJob(job: Job) {    
    jobManager.runJob(job)
  }
}

