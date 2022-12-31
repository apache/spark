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

package org.apache.spark.status.protobuf

import scala.collection.JavaConverters._

import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.status.{RDDOperationClusterWrapper, RDDOperationGraphWrapper}
import org.apache.spark.ui.scope.{RDDOperationEdge, RDDOperationNode}

class RDDOperationGraphWrapperSerializer extends ProtobufSerDe {

  override val supportClass: Class[_] = classOf[RDDOperationGraphWrapper]

  override def serialize(input: Any): Array[Byte] = {
    val op = input.asInstanceOf[RDDOperationGraphWrapper]
    val builder = StoreTypes.RDDOperationGraphWrapper.newBuilder()
    builder.setStageId(op.stageId.toLong)
    op.edges.foreach { e =>
      builder.addEdges(serializeRDDOperationEdge(e))
    }
    op.outgoingEdges.foreach { e =>
      builder.addOutgoingEdges(serializeRDDOperationEdge(e))
    }
    op.incomingEdges.foreach { e =>
      builder.addIncomingEdges(serializeRDDOperationEdge(e))
    }
    builder.setRootCluster(serializeRDDOperationClusterWrapper(op.rootCluster))
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): RDDOperationGraphWrapper = {
    val wrapper = StoreTypes.RDDOperationGraphWrapper.parseFrom(bytes)
    new RDDOperationGraphWrapper(
      stageId = wrapper.getStageId.toInt,
      edges = wrapper.getEdgesList.asScala.map(deserializeRDDOperationEdge).toSeq,
      outgoingEdges = wrapper.getOutgoingEdgesList.asScala.map(deserializeRDDOperationEdge).toSeq,
      incomingEdges = wrapper.getIncomingEdgesList.asScala.map(deserializeRDDOperationEdge).toSeq,
      rootCluster = deserializeRDDOperationClusterWrapper(wrapper.getRootCluster)
    )
  }

  private def serializeRDDOperationClusterWrapper(op: RDDOperationClusterWrapper):
    StoreTypes.RDDOperationClusterWrapper = {
    val builder = StoreTypes.RDDOperationClusterWrapper.newBuilder()
    builder.setId(op.id)
    builder.setName(op.name)
    op.childNodes.foreach { node =>
      builder.addChildNodes(serializeRDDOperationNode(node))
    }
    op.childClusters.foreach { cluster =>
      builder.addChildClusters(serializeRDDOperationClusterWrapper(cluster))
    }
    builder.build()
  }

  private def deserializeRDDOperationClusterWrapper(op: StoreTypes.RDDOperationClusterWrapper):
    RDDOperationClusterWrapper = {
    new RDDOperationClusterWrapper(
      id = op.getId,
      name = op.getName,
      childNodes = op.getChildNodesList.asScala.map(deserializeRDDOperationNode).toSeq,
      childClusters =
        op.getChildClustersList.asScala.map(deserializeRDDOperationClusterWrapper).toSeq
    )
  }

  private def serializeRDDOperationNode(node: RDDOperationNode): StoreTypes.RDDOperationNode = {
    val outputDeterministicLevel = StoreTypes.RDDOperationNode.DeterministicLevel
      .valueOf(node.outputDeterministicLevel.toString)
    val builder = StoreTypes.RDDOperationNode.newBuilder()
    builder.setId(node.id)
    builder.setName(node.name)
    builder.setCached(node.cached)
    builder.setBarrier(node.barrier)
    builder.setCallsite(node.callsite)
    builder.setOutputDeterministicLevel(outputDeterministicLevel)
    builder.build()
  }

  private def deserializeRDDOperationNode(node: StoreTypes.RDDOperationNode): RDDOperationNode = {
    RDDOperationNode(
      id = node.getId,
      name = node.getName,
      cached = node.getCached,
      barrier = node.getBarrier,
      callsite = node.getCallsite,
      outputDeterministicLevel =
        DeterministicLevel.withName(node.getOutputDeterministicLevel.toString)
    )
  }

  private def serializeRDDOperationEdge(edge: RDDOperationEdge): StoreTypes.RDDOperationEdge = {
    val builder = StoreTypes.RDDOperationEdge.newBuilder()
    builder.setFromId(edge.fromId)
    builder.setToId(edge.toId)
    builder.build()
  }

  private def deserializeRDDOperationEdge(edge: StoreTypes.RDDOperationEdge): RDDOperationEdge = {
    RDDOperationEdge(
      fromId = edge.getFromId,
      toId = edge.getToId)
  }
}
