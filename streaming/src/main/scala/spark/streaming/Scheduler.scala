package spark.streaming

import spark.streaming.util.RecurringTimer
import spark.SparkEnv
import spark.Logging

import scala.collection.mutable.HashMap


sealed trait SchedulerMessage
case class InputGenerated(inputName: String, interval: Interval, reference: AnyRef = null) extends SchedulerMessage
case class Test extends SchedulerMessage

class Scheduler(
    ssc: SparkStreamContext, 
    inputRDSs: Array[InputRDS[_]], 
    outputRDSs: Array[RDS[_]])
extends Logging {

  initLogging()

  val concurrentJobs = System.getProperty("spark.stream.concurrentJobs", "1").toInt
  val jobManager = new JobManager(ssc, concurrentJobs)
  val timer = new RecurringTimer(ssc.batchDuration, generateRDDs(_))
    
  def start() {
    
    val zeroTime = Time(timer.start())
    outputRDSs.foreach(_.initialize(zeroTime))
    inputRDSs.par.foreach(_.start())
    logInfo("Scheduler started")
  }
  
  def stop() {
    timer.stop()
    inputRDSs.par.foreach(_.stop())
    logInfo("Scheduler stopped")    
  }
  
  def generateRDDs (time: Time) {
    logInfo("Generating RDDs for time " + time) 
    outputRDSs.foreach(outputRDS => {
        outputRDS.generateJob(time) match {
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

