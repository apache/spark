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

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.status.{RDDOperationClusterWrapper, RDDOperationGraphWrapper}
import org.apache.spark.status.protobuf.StoreTypes.{DeterministicLevel => GDeterministicLevel}
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}
import org.apache.spark.ui.scope.{RDDOperationEdge, RDDOperationNode}

private[protobuf] class RDDOperationGraphWrapperSerializer
  extends ProtobufSerDe[RDDOperationGraphWrapper] {

  override def serialize(op: RDDOperationGraphWrapper): Array[Byte] = {
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
      edges = wrapper.getEdgesList.asScala.map(deserializeRDDOperationEdge),
      outgoingEdges = wrapper.getOutgoingEdgesList.asScala.map(deserializeRDDOperationEdge),
      incomingEdges = wrapper.getIncomingEdgesList.asScala.map(deserializeRDDOperationEdge),
      rootCluster = deserializeRDDOperationClusterWrapper(wrapper.getRootCluster)
    )
  }

  private def serializeRDDOperationClusterWrapper(op: RDDOperationClusterWrapper):
    StoreTypes.RDDOperationClusterWrapper = {
    val builder = StoreTypes.RDDOperationClusterWrapper.newBuilder()
    setStringField(op.id, builder.setId)
    setStringField(op.name, builder.setName)
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
      id = getStringField(op.hasId, op.getId),
      name = getStringField(op.hasName, op.getName),
      childNodes = op.getChildNodesList.asScala.map(deserializeRDDOperationNode),
      childClusters =
        op.getChildClustersList.asScala.map(deserializeRDDOperationClusterWrapper)
    )
  }

  private def serializeRDDOperationNode(node: RDDOperationNode): StoreTypes.RDDOperationNode = {
    val outputDeterministicLevel = DeterministicLevelSerializer.serialize(
      node.outputDeterministicLevel)
    val builder = StoreTypes.RDDOperationNode.newBuilder()
    builder.setId(node.id)
    setStringField(node.name, builder.setName)
    setStringField(node.callsite, builder.setCallsite)
    builder.setCached(node.cached)
    builder.setBarrier(node.barrier)
    builder.setOutputDeterministicLevel(outputDeterministicLevel)
    builder.build()
  }

  private def deserializeRDDOperationNode(node: StoreTypes.RDDOperationNode): RDDOperationNode = {
    RDDOperationNode(
      id = node.getId,
      name = getStringField(node.hasName, node.getName),
      cached = node.getCached,
      barrier = node.getBarrier,
      callsite = getStringField(node.hasCallsite, node.getCallsite),
      outputDeterministicLevel = DeterministicLevelSerializer.deserialize(
        node.getOutputDeterministicLevel)
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

private[protobuf] object DeterministicLevelSerializer {

  def serialize(input: DeterministicLevel.Value): GDeterministicLevel = {
    input match {
      case DeterministicLevel.DETERMINATE =>
        GDeterministicLevel.DETERMINISTIC_LEVEL_DETERMINATE
      case DeterministicLevel.UNORDERED =>
        GDeterministicLevel.DETERMINISTIC_LEVEL_UNORDERED
      case DeterministicLevel.INDETERMINATE =>
        GDeterministicLevel.DETERMINISTIC_LEVEL_INDETERMINATE
    }
  }

  def deserialize(binary: GDeterministicLevel): DeterministicLevel.Value = {
    binary match {
      case GDeterministicLevel.DETERMINISTIC_LEVEL_DETERMINATE =>
        DeterministicLevel.DETERMINATE
      case GDeterministicLevel.DETERMINISTIC_LEVEL_UNORDERED =>
        DeterministicLevel.UNORDERED
      case GDeterministicLevel.DETERMINISTIC_LEVEL_INDETERMINATE =>
        DeterministicLevel.INDETERMINATE
      case _ => null
    }
  }
}
