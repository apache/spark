package spark.streaming

import util.{ManualClock, RecurringTimer, Clock}
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
  val timer = new RecurringTimer(clock, ssc.graph.batchDuration, generateRDDs(_))

  def start() {
    // If context was started from checkpoint, then restart timer such that
    // this timer's triggers occur at the same time as the original timer.
    // Otherwise just start the timer from scratch, and initialize graph based
    // on this first trigger time of the timer.
    if (ssc.isCheckpointPresent) {
      // If manual clock is being used for testing, then
      // set manual clock to the last checkpointed time
      if (clock.isInstanceOf[ManualClock]) {
        val lastTime = ssc.getInitialCheckpoint.checkpointTime.milliseconds
        clock.asInstanceOf[ManualClock].setTime(lastTime)
      }
      timer.restart(graph.zeroTime.milliseconds)
      logInfo("Scheduler's timer restarted")
    } else {
      val zeroTime = Time(timer.start())
      graph.start(zeroTime)
      logInfo("Scheduler's timer started")
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
    graph.generateRDDs(time).foreach(submitJob)
    logInfo("Generated RDDs for time " + time)
    graph.forgetOldRDDs(time)
    if (ssc.checkpointInterval != null && (time - graph.zeroTime).isMultipleOf(ssc.checkpointInterval)) {
      ssc.doCheckpoint(time)
      logInfo("Checkpointed at time " + time)
    }
  }

  def submitJob(job: Job) {    
    jobManager.runJob(job)
  }
}

