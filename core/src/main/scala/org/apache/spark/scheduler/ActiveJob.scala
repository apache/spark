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

import java.util.Properties

import org.apache.spark.JobArtifactSet
import org.apache.spark.util.CallSite

/**
 * A running job in the DAGScheduler. Jobs can be of two types: a result job, which computes a
 * ResultStage to execute an action, or a map-stage job, which computes the map outputs for a
 * ShuffleMapStage before any downstream stages are submitted. The latter is used for adaptive
 * query planning, to look at map output statistics before submitting later stages. We distinguish
 * between these two types of jobs using the finalStage field of this class.
 *
 * Jobs are only tracked for "leaf" stages that clients directly submitted, through DAGScheduler's
 * submitJob or submitMapStage methods. However, either type of job may cause the execution of
 * other earlier stages (for RDDs in the DAG it depends on), and multiple jobs may share some of
 * these previous stages. These dependencies are managed inside DAGScheduler.
 *
 * @param jobId A unique ID for this job.
 * @param finalStage The stage that this job computes (either a ResultStage for an action or a
 *   ShuffleMapStage for submitMapStage).
 * @param callSite Where this job was initiated in the user's program (shown on UI).
 * @param listener A listener to notify if tasks in this job finish or the job fails.
 * @param artifacts A set of artifacts that this job has may use.
 * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
 */
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val callSite: CallSite,
    val listener: JobListener,
    val artifacts: JobArtifactSet,
    val properties: Properties) {

  /**
   * Number of partitions we need to compute for this job. Note that result stages may not need
   * to compute all partitions in their target RDD, for actions like first() and lookup().
   */
  val numPartitions = finalStage match {
    case r: ResultStage => r.partitions.length
    case m: ShuffleMapStage => m.numPartitions
  }

  /** Which partitions of the stage have finished */
  val finished = Array.fill[Boolean](numPartitions)(false)

  var numFinished = 0

  /**
   * Whether this job's RDD graph uses a `PipelinedShuffleDependency` (set once at job submission).
   * Every pipelined-group scheduling path -- co-scheduling, deferral, and the per-submit
   * `TaskSet.isPipelined` tagging -- is inert for a job without one, so this flag lets those paths
   * short-circuit the group-membership graph walk for the common regular job at no cost.
   */
  var hasPipelinedDependency: Boolean = false

  /**
   * True once this job's pipelined group has passed up-front gang admission in
   * `handleJobSubmitted` (or the slot check was disabled by config). This is a distinct fact from
   * `hasPipelinedDependency` (which only says the job uses pipelining): the co-schedule path in
   * `DAGScheduler.submitStage` gang-schedules a group's members with NO slot check, and asserts
   * this flag so that trust in up-front admission is enforced rather than merely commented.
   *
   * v1 ONLY: the whole job is a single pipelined group, so admission is a job-level fact. If a
   * later version allows multiple groups per job, this job-level flag must be replaced by
   * per-group admission state, and the assert in `submitStage` replaced by that per-group gate --
   * NOT simply deleted, or an unadmitted group could be gang-scheduled and deadlock.
   */
  var pipelinedGroupAdmitted: Boolean = false
}
