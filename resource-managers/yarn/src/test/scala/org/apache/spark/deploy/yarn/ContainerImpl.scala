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


package org.apache.spark.deploy.yarn

import java.util

import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ExecutionType, NodeId, Priority, Resource, Token}

class ContainerImpl(var containerId: ContainerId,
  var nodeId: NodeId,
  var nodeHttpAddress: String,
  var resource: Resource,
  var priority: Priority,
  var containerToken: Token,
  var executionType: ExecutionType,
  var allocationRequestId: Long,
  var allocationTags: util.Set[String]) extends Container {

  override def getId: ContainerId = containerId

  override def setId(containerId: ContainerId): Unit = this.containerId = containerId

  override def getNodeId: NodeId = nodeId

  override def setNodeId(nodeId: NodeId): Unit = this.nodeId = nodeId

  override def getNodeHttpAddress: String = nodeHttpAddress

  override def setNodeHttpAddress(s: String): Unit = this.nodeHttpAddress = s

  override def getResource: Resource = resource

  override def setResource(resource: Resource): Unit = this.resource = resource

  override def getPriority: Priority = priority

  override def setPriority(priority: Priority): Unit = this.priority = priority

  override def getContainerToken: Token = containerToken

  override def setContainerToken(token: Token): Unit = this.containerToken = token

  override def getExecutionType: ExecutionType = executionType

  override def setExecutionType(executionType: ExecutionType): Unit =
    this.executionType = executionType

  override def getAllocationRequestId: Long = allocationRequestId

  override def setAllocationRequestId(allocationRequestID: Long): Unit =
    this.allocationRequestId = allocationRequestID

  override def getAllocationTags: util.Set[String] = allocationTags

  override def setAllocationTags(allocationTags: util.Set[String]): Unit =
    this.allocationTags = allocationTags

  override def compareTo(other: Container): Int = {
    if (this.getId.compareTo(other.getId) == 0) {
      if (this.getNodeId.compareTo(other.getNodeId) == 0) {
        this.getResource.compareTo(other.getResource)
      } else {
        this.getNodeId.compareTo(other.getNodeId)
      }
    } else {
      this.getId.compareTo(other.getId)
    }
  }

  override def toString: String = s"ContainerImpl($containerId, $nodeId, $nodeHttpAddress, " +
    s"$resource, $priority, $containerToken, $executionType, $allocationRequestId, " +
    s"$allocationTags)"
}

