/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.scheduler

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._
import java.util.concurrent.{TimeUnit, ConcurrentHashMap, Executors}
import akka.actor.{ActorRef, Actor, Props}
import org.apache.spark.{SparkException, Logging, SparkEnv}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.streaming._


private[scheduler] sealed trait JobSchedulerEvent
private[scheduler] case class JobStarted(job: Job) extends JobSchedulerEvent
private[scheduler] case class JobCompleted(job: Job) extends JobSchedulerEvent
private[scheduler] case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent

/**
 * This class schedules jobs to be run on Spark. It uses the JobGenerator to generate
 * the jobs and runs them using a thread pool.
 */
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {

  private val jobSets = new ConcurrentHashMap[Time, JobSet]
  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
  private val jobExecutor = Executors.newFixedThreadPool(numConcurrentJobs)
  private val jobGenerator = new JobGenerator(this)
  val clock = jobGenerator.clock
  val listenerBus = new StreamingListenerBus()

  // These two are created only when scheduler starts.
  // eventActor not being null means the scheduler has been started and not stopped
  var receiverTracker: ReceiverTracker = null
  private var eventActor: ActorRef = null


  def start(): Unit = synchronized {
    if (eventActor != null) return // scheduler has already been started

    logDebug("Starting JobScheduler")
    eventActor = ssc.env.actorSystem.actorOf(Props(new Actor {
      def receive = {
        case event: JobSchedulerEvent => processEvent(event)
      }
    }), "JobScheduler")

    listenerBus.start(ssc.sparkContext)
    receiverTracker = new ReceiverTracker(ssc)
    receiverTracker.start()
    jobGenerator.start()
    logInfo("Started JobScheduler")
  }

  def stop(processAllReceivedData: Boolean): Unit = synchronized {
    if (eventActor == null) return // scheduler has already been stopped
    logDebug("Stopping JobScheduler")

    // First, stop receiving
    receiverTracker.stop(processAllReceivedData)

    // Second, stop generating jobs. If it has to process all received data,
    // then this will wait for all the processing through JobScheduler to be over.
    jobGenerator.stop(processAllReceivedData)

    // Stop the executor for receiving new jobs
    logDebug("Stopping job executor")
    jobExecutor.shutdown()

    // Wait for the queued jobs to complete if indicated
    val terminated = if (processAllReceivedData) {
      jobExecutor.awaitTermination(1, TimeUnit.HOURS)  // just a very large period of time
    } else {
      jobExecutor.awaitTermination(2, TimeUnit.SECONDS)
    }
    if (!terminated) {
      jobExecutor.shutdownNow()
    }
    logDebug("Stopped job executor")

    // Stop everything else
    listenerBus.stop()
    ssc.env.actorSystem.stop(eventActor)
    eventActor = null
    logInfo("Stopped JobScheduler")
  }

  def submitJobSet(jobSet: JobSet) {
    if (jobSet.jobs.isEmpty) {
      logInfo("No jobs added for time " + jobSet.time)
    } else {
      jobSets.put(jobSet.time, jobSet)
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
      logInfo("Added jobs for time " + jobSet.time)
    }
  }

  def getPendingTimes(): Seq[Time] = {
    jobSets.keySet.toSeq
  }

  def reportError(msg: String, e: Throwable) {
    eventActor ! ErrorReported(msg, e)
  }

  private def processEvent(event: JobSchedulerEvent) {
    try {
      event match {
        case JobStarted(job) => handleJobStart(job)
        case JobCompleted(job) => handleJobCompletion(job)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
  }

  private def handleJobStart(job: Job) {
    val jobSet = jobSets.get(job.time)
    if (!jobSet.hasStarted) {
      listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
    }
    jobSet.handleJobStart(job)
    logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
  }

  private def handleJobCompletion(job: Job) {
    job.result match {
      case Success(_) =>
        val jobSet = jobSets.get(job.time)
        jobSet.handleJobCompletion(job)
        logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
        if (jobSet.hasCompleted) {
          jobSets.remove(jobSet.time)
          jobGenerator.onBatchCompletion(jobSet.time)
          logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
            jobSet.totalDelay / 1000.0, jobSet.time.toString,
            jobSet.processingDelay / 1000.0
          ))
          listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))
        }
      case Failure(e) =>
        reportError("Error running job " + job, e)
    }
  }

  private def handleError(msg: String, e: Throwable) {
    logError(msg, e)
    ssc.waiter.notifyError(e)
  }

  private class JobHandler(job: Job) extends Runnable {
    def run() {
      eventActor ! JobStarted(job)
      // Disable checks for existing output directories in jobs launched by the streaming scheduler,
      // since we may need to write output to an existing directory during checkpoint recovery;
      // see SPARK-4835 for more details.
      PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
        job.run()
      }
      eventActor ! JobCompleted(job)
    }
  }
}
