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
  private val executor = Executors.newFixedThreadPool(numConcurrentJobs)
  private val jobGenerator = new JobGenerator(this)
  val clock = jobGenerator.clock
  val listenerBus = new StreamingListenerBus()

  // These two are created only when scheduler starts.
  // eventActor not being null means the scheduler has been started and not stopped
  var networkInputTracker: NetworkInputTracker = null
  private var eventActor: ActorRef = null


  def start() = synchronized {
    if (eventActor != null) {
      throw new SparkException("JobScheduler already started")
    }

    eventActor = ssc.env.actorSystem.actorOf(Props(new Actor {
      def receive = {
        case event: JobSchedulerEvent => processEvent(event)
      }
    }), "JobScheduler")
    listenerBus.start()
    networkInputTracker = new NetworkInputTracker(ssc)
    networkInputTracker.start()
    Thread.sleep(1000)
    jobGenerator.start()
    logInfo("JobScheduler started")
  }

  def stop() = synchronized {
    if (eventActor != null) {
      jobGenerator.stop()
      networkInputTracker.stop()
      executor.shutdown()
      if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
        executor.shutdownNow()
      }
      listenerBus.stop()
      ssc.env.actorSystem.stop(eventActor)
      logInfo("JobScheduler stopped")
    }
  }

  def runJobs(time: Time, jobs: Seq[Job]) {
    if (jobs.isEmpty) {
      logInfo("No jobs added for time " + time)
    } else {
      val jobSet = new JobSet(time, jobs)
      jobSets.put(time, jobSet)
      jobSet.jobs.foreach(job => executor.execute(new JobHandler(job)))
      logInfo("Added jobs for time " + time)
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
    SparkEnv.set(ssc.env)
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
      job.run()
      eventActor ! JobCompleted(job)
    }
  }
}
