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

package org.apache.spark.streaming

import org.apache.spark.Logging
import org.apache.spark.SparkEnv
import java.util.concurrent.Executors
import collection.mutable.HashMap
import collection.mutable.ArrayBuffer


private[streaming]
class JobManager(ssc: StreamingContext, numThreads: Int = 1) extends Logging {
  
  class JobHandler(ssc: StreamingContext, job: Job) extends Runnable {
    def run() {
      SparkEnv.set(ssc.env)
      try {
        val timeTaken = job.run()
        logInfo("Total delay: %.5f s for job %s of time %s (execution: %.5f s)".format(
          (System.currentTimeMillis() - job.time.milliseconds) / 1000.0, job.id, job.time.milliseconds, timeTaken / 1000.0))
      } catch {
        case e: Exception =>
          logError("Running " + job + " failed", e)
      }
      clearJob(job)
    }
  }

  initLogging()

  val jobExecutor = Executors.newFixedThreadPool(numThreads) 
  val jobs = new HashMap[Time, ArrayBuffer[Job]]

  def runJob(job: Job) {
    jobs.synchronized {
      jobs.getOrElseUpdate(job.time, new ArrayBuffer[Job]) += job
    }
    jobExecutor.execute(new JobHandler(ssc, job))
    logInfo("Added " + job + " to queue")
  }

  def stop() {
    jobExecutor.shutdown()
  }

  private def clearJob(job: Job) {
    var timeCleared = false
    val time = job.time
    jobs.synchronized {
      val jobsOfTime = jobs.get(time)
      if (jobsOfTime.isDefined) {
        jobsOfTime.get -= job
        if (jobsOfTime.get.isEmpty) {
          jobs -= time
          timeCleared = true
        }
      } else {
        throw new Exception("Job finished for time " + job.time +
          " but time does not exist in jobs")
      }
    }
    if (timeCleared) {
      ssc.scheduler.clearOldMetadata(time)
    }
  }

  def getPendingTimes(): Array[Time] = {
    jobs.synchronized {
      jobs.keySet.toArray
    }
  }
}
