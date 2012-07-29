package spark.streaming

import spark.SparkEnv
import spark.Logging

import scala.collection.mutable.PriorityQueue
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.actors.scheduler.ResizableThreadPoolScheduler
import scala.actors.scheduler.ForkJoinScheduler

sealed trait JobManagerMessage
case class RunJob(job: Job) extends JobManagerMessage
case class JobCompleted(handlerId: Int) extends JobManagerMessage

class JobHandler(ssc: SparkStreamContext, val id: Int) extends DaemonActor with Logging {

  var busy = false

  def act() { 
    loop {
      receive {
        case job: Job => { 
          SparkEnv.set(ssc.env)
          try {
            logInfo("Starting "  + job)
            job.run()
            logInfo("Finished " + job)
            if (job.time.isInstanceOf[LongTime]) {
              val longTime = job.time.asInstanceOf[LongTime]
              logInfo("Total pushing + skew + processing delay for " + longTime + " is " +
                (System.currentTimeMillis - longTime.milliseconds) / 1000.0  + " s")
            }
          } catch {
            case e: Exception => logError("SparkStream job failed", e)
          }
          busy = false
          reply(JobCompleted(id))
        }
      }
    }
  }
}

class JobManager(ssc: SparkStreamContext, numThreads: Int = 2) extends DaemonActor with Logging {

  implicit private val jobOrdering = new Ordering[Job] {
    override def compare(job1: Job, job2: Job): Int = {
      if (job1.time < job2.time) {
        return 1
      } else if (job2.time < job1.time) {
        return -1
      } else {
        return 0
      }
    }
  }

  private val jobs = new PriorityQueue[Job]()
  private val handlers = (0 until numThreads).map(i => new JobHandler(ssc, i))

  def act() {
    handlers.foreach(_.start)
    loop {
      receive {
        case RunJob(job) => {
          jobs += job
          logInfo("Job " + job + " submitted")
          runJob()
        }
        case JobCompleted(handlerId) => {
          runJob()
        }
      }
    }
  }

  def runJob(): Unit =  {
    logInfo("Attempting to allocate job ")
    if (jobs.size > 0) {
      handlers.find(!_.busy).foreach(handler => {
        val job = jobs.dequeue
        logInfo("Allocating job " + job + " to handler " + handler.id)
        handler.busy = true
        handler ! job
      })
    } 
  }
}

object JobManager {
  def main(args: Array[String]) {
    val ssc = new SparkStreamContext("local[4]", "JobManagerTest")
    val jobManager = new JobManager(ssc) 
    jobManager.start()
    
    val t = System.currentTimeMillis
    for (i <- 1 to 10) {
      jobManager ! RunJob(new Job(
        LongTime(i), 
        () => {
          Thread.sleep(500)
          println("Job " + i + " took " + (System.currentTimeMillis - t) + " ms")
        }
      ))
    }
    Thread.sleep(6000)
  }
}

