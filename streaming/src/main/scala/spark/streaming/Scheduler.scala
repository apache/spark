package spark.streaming

import util.{ManualClock, RecurringTimer, Clock}
import spark.SparkEnv
import spark.Logging

import scala.collection.mutable.HashMap


private[streaming]
class Scheduler(ssc: StreamingContext) extends Logging {

  initLogging()

  val graph = ssc.graph

  val concurrentJobs = System.getProperty("spark.streaming.concurrentJobs", "1").toInt
  val jobManager = new JobManager(ssc, concurrentJobs)

  val checkpointWriter = if (ssc.checkpointInterval != null && ssc.checkpointDir != null) {
    new CheckpointWriter(ssc.checkpointDir)
  } else {
    null
  }

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
      // either set the manual clock to the last checkpointed time,
      // or if the property is defined set it to that time
      if (clock.isInstanceOf[ManualClock]) {
        val lastTime = ssc.getInitialCheckpoint.checkpointTime.milliseconds
        val jumpTime = System.getProperty("spark.streaming.manualClock.jump", "0").toLong
        clock.asInstanceOf[ManualClock].setTime(lastTime + jumpTime)
      }
      timer.restart(graph.zeroTime.milliseconds)
      logInfo("Scheduler's timer restarted")
    } else {
      val firstTime = Time(timer.start())
      graph.start(firstTime - ssc.graph.batchDuration)
      logInfo("Scheduler's timer started")
    }
    logInfo("Scheduler started")
  }
  
  def stop() {
    timer.stop()
    graph.stop()
    logInfo("Scheduler stopped")    
  }
  
  private def generateRDDs(time: Time) {
    SparkEnv.set(ssc.env)
    logInfo("\n-----------------------------------------------------\n")
    graph.generateRDDs(time).foreach(jobManager.runJob)
    graph.forgetOldRDDs(time)
    doCheckpoint(time)
    logInfo("Generated RDDs for time " + time)
  }

  private def doCheckpoint(time: Time) {
    if (ssc.checkpointInterval != null && (time - graph.zeroTime).isMultipleOf(ssc.checkpointInterval)) {
      val startTime = System.currentTimeMillis()
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time))
      val stopTime = System.currentTimeMillis()
      logInfo("Checkpointing the graph took " + (stopTime - startTime) + " ms")
    }
  }
}

