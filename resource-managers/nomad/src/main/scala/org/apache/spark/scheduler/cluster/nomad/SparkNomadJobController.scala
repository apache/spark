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

package org.apache.spark.scheduler.cluster.nomad

import com.hashicorp.nomad.apimodel._
import com.hashicorp.nomad.scalasdk.NomadScalaApi

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.nomad.SparkNomadJob.CommonConf

/**
 * Manipulates a Nomad job running a Spark application.
 *
 * A single Nomad job is used for a Spark application running with a Nomad master.
 * This includes all of the executors (and possibly shuffle servers),
 * and in cluster mode also includes the Spark driver process.
 */
private[spark] class SparkNomadJobController(jobManipulator: NomadJobManipulator) extends Logging {

  def jobId: String = jobManipulator.jobId

  def startDriver(): Evaluation = {
    logInfo(s"Starting driver in Nomad job ${jobId}")
    jobManipulator.create()
  }

  def fetchDriverLogUrls(conf: SparkConf): Option[Map[String, String]] = {
    for {
      allocId <- Option(conf.getenv("NOMAD_ALLOC_ID"))
      taskName <- Option(conf.getenv("NOMAD_TASK_NAME"))
    } yield jobManipulator.fetchLogUrlsForTask(allocId, taskName)
  }

  def initialiseExecutors(
      conf: SparkConf,
      driverUrl: String,
      count: Int
  ): Unit = {
    jobManipulator.updateJob(startIfNotYetRunning = count > 0) { job =>
      val group = SparkNomadJob.find(job, ExecutorTaskGroup).get
      ExecutorTaskGroup.initialize(conf, group, driverUrl, count)
    }
  }

  def setExecutorCount(count: Int): Unit = {
    jobManipulator.updateJob(startIfNotYetRunning = count > 0) { job =>
      SparkNomadJob.find(job, ExecutorTaskGroup).get
        .setCount(count)
    }
  }

  def resolveExecutorLogUrls(reportedLogUrls: Map[String, String]): Map[String, String] =
    reportedLogUrls.get(ExecutorTask.LOG_KEY_FOR_ALLOC_ID) match {
      case None =>
        logWarning(s"Didn't find expected ${ExecutorTask.LOG_KEY_FOR_ALLOC_ID} key in " +
          s"executor log URLs: $reportedLogUrls")
        reportedLogUrls
      case Some(allocId) =>
          jobManipulator.fetchLogUrlsForTask(allocId, "executor")
    }

  def close(): Unit = {
    jobManipulator.close()
  }

}

private[spark] object SparkNomadJobController extends Logging {

  def initialize(
      clusterModeConf: NomadClusterManagerConf,
      nomad: NomadScalaApi
  ): SparkNomadJobController = {
    try {
      val manipulator = NomadJobManipulator.fetchOrCreateJob(nomad, clusterModeConf.jobDescriptor)
      new SparkNomadJobController(manipulator)
    } catch {
      case e: Throwable =>
        nomad.close()
        throw e
    }
  }

}
