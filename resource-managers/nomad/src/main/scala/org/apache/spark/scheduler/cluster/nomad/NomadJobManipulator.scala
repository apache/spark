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

import scala.annotation.tailrec

import com.hashicorp.nomad.apimodel.{Evaluation, Job, Node}
import com.hashicorp.nomad.javasdk.{ErrorResponseException, WaitStrategy}
import com.hashicorp.nomad.scalasdk.NomadScalaApi

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.nomad.NomadClusterManagerConf.{ExistingJob, JobDescriptor, NewJob}

/**
 * Convenience interface for manipulating a Nomad job.
 */
private[spark] class NomadJobManipulator(val nomad: NomadScalaApi, private var job: Job)
  extends Logging {

  def jobId: String = job.getId

  def create(): Evaluation = {
    register()
  }

  @tailrec
  final def updateJob(startIfNotYetRunning: Boolean)(modify: Job => Unit): Unit = {
    modify(job)
    logDebug("Trying to update to " + job)
    try {
      register()
    } catch {
      case e: ErrorResponseException =>
        log.warn(s"Updating job and retrying modification after error: $e")
        val response = nomad.jobs.info(jobId)
        job = response.getValue
        updateJob(startIfNotYetRunning)(modify)
    }
  }

  def fetchLogUrlsForTask(allocId: String, task: String): Map[String, String] = {
    val allocation = nomad.allocations.info(allocId).getValue
    val node = nomad.nodes.info(allocation.getNodeId).getValue
    Map(
      "stdout" -> logUrl(node, allocId, task, "stdout"),
      "stderr" -> logUrl(node, allocId, task, "stderr")
    )
  }

  def logUrl(node: Node, allocId: String, task: String, log: String): String = {
    val baseUrl = nomadHttpBaseUrl(node)
    s"$baseUrl/v1/client/fs/logs/$allocId?follow=true&plain=true&task=$task&type=$log"
  }

  def close(): Unit = {
    logInfo(s"Closing Nomad API")
    try nomad.close()
    finally logInfo(s"Nomad API closed")
  }

  protected def register(): Evaluation = {
    val oldIndex = job.getJobModifyIndex
    val evaluation =
      nomad.evaluations.pollForCompletion(
        nomad.jobs.register(job, modifyIndex = Some(oldIndex)),
        WaitStrategy.WAIT_INDEFINITELY
      ).getValue
    val newIndex = evaluation.getJobModifyIndex
    log.info(s"Registered Nomad job $jobId (job modify index $oldIndex -> $newIndex)")
    job.setJobModifyIndex(newIndex)
    evaluation
  }

  private[this] def nomadHttpBaseUrl(node: Node): String =
    (if (node.getTlsEnabled) "https://" else "http://") + node.getHttpAddr

}

private[spark] object NomadJobManipulator extends Logging {

  def fetchOrCreateJob(nomad: NomadScalaApi, jobDescriptor: JobDescriptor): NomadJobManipulator = {

    val job = jobDescriptor match {

      case ExistingJob(jobId, _) =>
        logInfo(s"Fetching current state of existing Nomad job $jobId")
        val response = nomad.jobs.info(jobId)
        logDebug(s"State: ${response.getRawEntity}")
        response.getValue

      case NewJob(job) =>
        job
    }

    new NomadJobManipulator(nomad, job)
  }

}
