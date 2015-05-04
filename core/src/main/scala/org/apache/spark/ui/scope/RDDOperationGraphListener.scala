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

package org.apache.spark.ui.scope

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI

/**
 * A SparkListener that constructs a DAG of RDD operations.
 */
private[ui] class RDDOperationGraphListener(conf: SparkConf) extends SparkListener {
  private val jobIdToStageIds = new mutable.HashMap[Int, Seq[Int]]
  private val stageIdToGraph = new mutable.HashMap[Int, RDDOperationGraph]
  private val stageIds = new mutable.ArrayBuffer[Int]

  // How many jobs or stages to retain graph metadata for
  private val retainedStages =
    conf.getInt("spark.ui.retainedStages", SparkUI.DEFAULT_RETAINED_STAGES)

  /** Return the graph metadata for the given stage, or None if no such information exists. */
  def getOperationGraphForJob(jobId: Int): Seq[RDDOperationGraph] = {
    jobIdToStageIds.get(jobId)
      .map { sids => sids.flatMap { sid => stageIdToGraph.get(sid) } }
      .getOrElse { Seq.empty }
  }

  /** Return the graph metadata for the given stage, or None if no such information exists. */
  def getOperationGraphForStage(stageId: Int): Option[RDDOperationGraph] = {
    stageIdToGraph.get(stageId)
  }

  /** On job start, construct a RDDOperationGraph for each stage in the job for display later. */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val jobId = jobStart.jobId
    val stageInfos = jobStart.stageInfos

    stageInfos.foreach { stageInfo =>
      stageIds += stageInfo.stageId
      stageIdToGraph(stageInfo.stageId) = RDDOperationGraph.makeOperationGraph(stageInfo)
    }
    jobIdToStageIds(jobId) = stageInfos.map(_.stageId).sorted

    // Remove graph metadata for old stages
    if (stageIds.size >= retainedStages) {
      val toRemove = math.max(retainedStages / 10, 1)
      stageIds.take(toRemove).foreach { id => stageIdToGraph.remove(id) }
      stageIds.trimStart(toRemove)
    }
  }
}
