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

package org.apache.spark.ui.viz

import scala.collection.mutable
import scala.xml.{Node, Unparsed}

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI

/**
 * A SparkListener that constructs RDD DAG visualization for the UI.
 */
private[ui] class VisualizationListener(conf: SparkConf) extends SparkListener {
  private val jobIdToStageIds = new mutable.HashMap[Int, Seq[Int]]
  private val stageIdToGraph = new mutable.HashMap[Int, VizGraph]
  private val stageIds = new mutable.ArrayBuffer[Int]

  // How many jobs or stages to retain graph metadata for
  private val retainedStages =
    conf.getInt("spark.ui.retainedStages", SparkUI.DEFAULT_RETAINED_STAGES)

  /** Construct a "Show visualization" DOM element that expands into a visualization for a stage. */
  def showVizElementForStage(stageId: Int): Seq[Node] = {
    showVizElement(getVizGraphForStage(stageId).toSeq, forJob = false)
  }

  /** Construct a "Show visualization" DOM element that expands into a visualization for a job. */
  def showVizElementForJob(jobId: Int): Seq[Node] = {
    showVizElement(getVizGraphsForJob(jobId), forJob = true)
  }

  /** Construct a "Show visualization" DOM element that expands into a visualization on the UI. */
  private def showVizElement(graphs: Seq[VizGraph], forJob: Boolean): Seq[Node] = {
    <div>
      <span class="expand-visualization" onclick="render();">
        <span class="expand-visualization-arrow arrow-closed"></span>
        <strong>Show visualization</strong>
      </span>
      <div id="viz-graph"></div>
      <div id="viz-dot-files">
        {
          graphs.map { g =>
            <div name={g.rootScope.id} style="display:none">
              <div class="dot-file">{VizGraph.makeDotFile(g, forJob)}</div>
              { g.incomingEdges.map { e => <div class="incoming-edge">{e.fromId},{e.toId}</div> } }
              { g.outgoingEdges.map { e => <div class="outgoing-edge">{e.fromId},{e.toId}</div> } }
            </div>
          }
        }
      </div>
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"function render() { toggleShowViz($forJob); }")}
    </script>
  }

  /** Return the graph metadata for the given stage, or None if no such information exists. */
  private def getVizGraphsForJob(jobId: Int): Seq[VizGraph] = {
    jobIdToStageIds.get(jobId)
      .map { sids => sids.flatMap { sid => stageIdToGraph.get(sid) } }
      .getOrElse { Seq.empty }
  }

  /** Return the graph metadata for the given stage, or None if no such information exists. */
  private def getVizGraphForStage(stageId: Int): Option[VizGraph] = {
    stageIdToGraph.get(stageId)
  }

  /** On job start, construct a VizGraph for each stage in the job for display later. */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val jobId = jobStart.jobId
    val stageInfos = jobStart.stageInfos

    stageInfos.foreach { stageInfo =>
      stageIds += stageInfo.stageId
      stageIdToGraph(stageInfo.stageId) = VizGraph.makeVizGraph(stageInfo)
    }
    jobIdToStageIds(jobId) = stageInfos.map(_.stageId)

    // Remove graph metadata for old stages
    if (stageIds.size >= retainedStages) {
      val toRemove = math.max(retainedStages / 10, 1)
      stageIds.take(toRemove).foreach { id => stageIdToGraph.remove(id) }
      stageIds.trimStart(toRemove)
    }
  }
}
