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

import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI

/**
 * A SparkListener that constructs a graph of the RDD DAG for each stage.
 * This graph will be used for rendering visualization in the UI later.
 */
private[ui] class VisualizationListener extends SparkListener {

  // A list of stage IDs to track the order in which stages are inserted
  private val stageIds = new mutable.ArrayBuffer[Int]

  // Stage ID -> graph metadata for the stage
  private val stageIdToGraph = new mutable.HashMap[Int, VizGraph]

  // How many stages to retain graph metadata for
  private val retainedStages =
    conf.getInt("spark.ui.retainedStages", SparkUI.DEFAULT_RETAINED_STAGES)

  /** Return the graph metadata for the given stage, or None if no such information exists. */
  def getVizGraph(stageId: Int): Option[VizGraph] = {
    stageIdToGraph.get(stageId)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageCompleted.stageInfo.stageId
    val rddInfos = stageCompleted.stageInfo.rddInfos
    val vizGraph = VizGraph.makeVizGraph(rddInfos)
    stageIdToGraph(stageId) = vizGraph

    // Remove metadata for old stages
    if (stageIds.size >= retainedStages) {
      val toRemove = math.max(retainedStages / 10, 1)
      stageIds.take(toRemove).foreach { id => stageIdToGraph.remove(id) }
      stageIds.trimStart(toRemove)
    }
  }
}
