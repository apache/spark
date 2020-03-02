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

package org.apache.spark.scheduler.cluster

import java.text.SimpleDateFormat

import scala.collection.mutable.{HashMap}

import org.apache.hadoop.yarn.api.records.{NodeState => YarnNodeState}

/**
 * State of the node.
 * Add the node state depending upon the cluster manager, For Yarn
 * getYarnNodeState is added to create the node state for Decommission Tracker
 */
private[spark] object NodeState extends Enumeration {
  val RUNNING, DECOMMISSIONED, GRACEFUL_DECOMMISSIONING, DECOMMISSIONING, LOST, OTHER = Value
  type NodeState = Value

  // Helper method to get NodeState of the Yarn.
  def getYarnNodeState(state: YarnNodeState): NodeState.Value = {
    // In hadoop-2.7 there is no support for node state DECOMMISSIONING
    // In Hadoop-2.8, hadoop3.1 and later version of spark there is a support
    // to node state DECOMMISSIONING.
    // Inorder to build the spark using hadoop2 and hadoop3, not
    // using YarnNodeState for the node state DECOMMISSIONING here and
    // and for other state we are matching the YarnNodeState and assigning
    // the node state at spark end
    if (state.toString.equals(NodeState.DECOMMISSIONING.toString)) {
      NodeState.GRACEFUL_DECOMMISSIONING
    } else {
      state match {
        case YarnNodeState.RUNNING => NodeState.RUNNING
        case YarnNodeState.DECOMMISSIONED => NodeState.DECOMMISSIONED
        case YarnNodeState.LOST => NodeState.LOST
        case YarnNodeState.UNHEALTHY => NodeState.LOST
        case _ => NodeState.OTHER
      }
    }
  }
}

/**
 * Node information used by CoarseGrainedSchedulerBackend.
 *
 * @param nodeState node state
 * @param terminationTime time at which node will terminate
 */
private[spark] case class NodeInfo(nodeState: NodeState.Value, terminationTime: Option[Long])
    extends Serializable {
  override def toString(): String = {
    val df: SimpleDateFormat = new SimpleDateFormat("YY/MM/dd HH:mm:ss")
    val tDateTime = df.format(terminationTime.getOrElse(0L))
    s""""terminationTime":"$tDateTime","state":"$nodeState""""
  }
}

/**
 * Cluster status information used by CoarseGrainedSchedulerBackend.
 *
 * @param nodeInfos Information about node about to be decommissioned
 * resource in the cluster.
 */
private[spark] case class ClusterInfo(
    nodeInfos: HashMap[String, NodeInfo])
    extends Serializable {
  override def toString(): String = {

    var nodeInfoStr = ""
    nodeInfos.foreach {
      case (k, v) =>
        nodeInfoStr = nodeInfoStr + s"""{"node":"$k",$v}"""
    }
    if (nodeInfoStr == "") {
      s"""{"nodeInfos":{}}"""
    } else {
      s"""{"nodeInfos":$nodeInfoStr}"""
    }
  }
}
