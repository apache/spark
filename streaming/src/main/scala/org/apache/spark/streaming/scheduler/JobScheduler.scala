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

import org.apache.spark.Logging
import org.apache.spark.SparkEnv
import java.util.concurrent.{TimeUnit, ConcurrentHashMap, Executors}
import scala.collection.mutable.HashSet
import org.apache.spark.streaming._

/**
 * This class schedules jobs to be run on Spark. It uses the JobGenerator to generate
 * the jobs and runs them using a thread pool. Number of threads
 */
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {

  val jobSets = new ConcurrentHashMap[Time, JobSet]
  val numConcurrentJobs = ssc.conf.get("spark.streaming.concurrentJobs", "1").toInt
  val executor = Executors.newFixedThreadPool(numConcurrentJobs)
  val generator = new JobGenerator(this)
  val listenerBus = new StreamingListenerBus()

  def clock = generator.clock

  def start() {
    generator.start()
  }

  def stop() {
    generator.stop()
    executor.shutdown()
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      executor.shutdownNow()
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

  def getPendingTimes(): Array[Time] = {
    jobSets.keySet.toArray(new Array[Time](0))
  }

  private def beforeJobStart(job: Job) {
    val jobSet = jobSets.get(job.time)
    if (!jobSet.hasStarted) {
      listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo()))
    }
    jobSet.beforeJobStart(job)
    logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
    SparkEnv.set(generator.ssc.env)
  }

  private def afterJobEnd(job: Job) {
    val jobSet = jobSets.get(job.time)
    jobSet.afterJobStop(job)
    logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
    if (jobSet.hasCompleted) {
      jobSets.remove(jobSet.time)
      generator.onBatchCompletion(jobSet.time)
      logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
        jobSet.totalDelay / 1000.0, jobSet.time.toString,
        jobSet.processingDelay / 1000.0
      ))
      listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo()))
    }
  }

  private[streaming]
  class JobHandler(job: Job) extends Runnable {
    def run() {
      beforeJobStart(job)
      try {
        job.run()
      } catch {
        case e: Exception =>
          logError("Running " + job + " failed", e)
      }
      afterJobEnd(job)
    }
  }
}
