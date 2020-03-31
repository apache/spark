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

/**
 * State of the node.
 * Add the node state depending upon the cluster manager, For Yarn
 * getYarnNodeState is added to create the node state for Decommission Tracker
 */
private[spark] object NodeState extends Enumeration {
  val RUNNING, DECOMMISSIONED, DECOMMISSIONING, LOST, OTHER = Value
  type NodeState = Value
}

/**
 * Node information used by CoarseGrainedSchedulerBackend.
 *
 * @param nodeState node state
 * @param terminationTime Time at which node will terminate.
 *   This optional and will be set only for node state DECOMMISSIONING
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
