package spark.streaming

import spark.streaming.util.RecurringTimer
import spark.streaming.util.Clock
import spark.SparkEnv
import spark.Logging

import scala.collection.mutable.HashMap


sealed trait SchedulerMessage
case class InputGenerated(inputName: String, interval: Interval, reference: AnyRef = null) extends SchedulerMessage

class Scheduler(ssc: StreamingContext)
extends Logging {

  initLogging()

  val graph = ssc.graph
  val concurrentJobs = System.getProperty("spark.stream.concurrentJobs", "1").toInt
  val jobManager = new JobManager(ssc, concurrentJobs)
  val clockClass = System.getProperty("spark.streaming.clock", "spark.streaming.util.SystemClock")
  val clock = Class.forName(clockClass).newInstance().asInstanceOf[Clock]
  val timer = new RecurringTimer(clock, ssc.batchDuration, generateRDDs(_))


  def start() {
    if (graph.started) {
      timer.restart(graph.zeroTime.milliseconds)
    } else {
      val zeroTime = Time(timer.start())
      graph.start(zeroTime)
    }
    logInfo("Scheduler started")
  }
  
  def stop() {
    timer.stop()
    graph.stop()
    logInfo("Scheduler stopped")    
  }
  
  def generateRDDs(time: Time) {
    SparkEnv.set(ssc.env)
    logInfo("\n-----------------------------------------------------\n")
    logInfo("Generating RDDs for time " + time)
    graph.generateRDDs(time).foreach(submitJob)
    logInfo("Generated RDDs for time " + time)
    if (ssc.checkpointInterval != null && (time - graph.zeroTime).isMultipleOf(ssc.checkpointInterval)) {
      ssc.checkpoint()
    }
  }

  def submitJob(job: Job) {    
    jobManager.runJob(job)
  }
}

