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

package org.apache.spark.scheduler

import scala.collection.mutable.HashMap

import org.apache.spark.internal.Logging

/**
 * JobCleanedHookListener is a basic job cleaned listener. It holds jobCleanedHooks for
 * jobs and run cleaned hook after a job is cleaned.
 */
class JobCleanedHookListener extends SparkListener with Logging {
  // use private val and synchronized to keep thread safe
  private val jobCleanedHooks = new HashMap[Int, Int => Unit]()

  def addCleanedHook(jobId: Int, fun: Int => Unit): Unit = synchronized{
    jobCleanedHooks.put(jobId, fun)
  }

  def deleteCleanedHook(jobId: Int): Unit = synchronized {
    jobCleanedHooks.remove(jobId)
  }

  override def onJobCleaned(jobCleaned: SparkListenerJobCleaned): Unit = {
    jobCleanedHooks.get(jobCleaned.jobId)
      .foreach{function =>
        function(jobCleaned.jobId)
        deleteCleanedHook(jobCleaned.jobId)
      }
  }
}
