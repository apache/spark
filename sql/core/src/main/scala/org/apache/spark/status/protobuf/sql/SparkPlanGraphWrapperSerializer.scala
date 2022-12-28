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

package org.apache.spark.status.protobuf.sql

import collection.JavaConverters._

import org.apache.spark.sql.execution.ui.{SparkPlanGraphClusterWrapper, SparkPlanGraphEdge, SparkPlanGraphNode, SparkPlanGraphNodeWrapper, SparkPlanGraphWrapper}
import org.apache.spark.status.protobuf.ProtobufSerDe
import org.apache.spark.status.protobuf.StoreTypes

class SparkPlanGraphWrapperSerializer extends ProtobufSerDe {

  override val supportClass: Class[_] = classOf[SparkPlanGraphWrapper]

  override def serialize(input: Any): Array[Byte] = {
    val plan = input.asInstanceOf[SparkPlanGraphWrapper]
    val builder = StoreTypes.SparkPlanGraphWrapper.newBuilder()
    builder.setExecutionId(plan.executionId)
    plan.nodes.foreach { node =>
      builder.addNodes(serializeSparkPlanGraphNodeWrapper(node))
    }
    plan.edges.foreach {edge =>
      builder.addEdges(serializeSparkPlanGraphEdge(edge))
    }
    builder.build().toByteArray
  }

  def deserialize(bytes: Array[Byte]): SparkPlanGraphWrapper = {
    val wrapper = StoreTypes.SparkPlanGraphWrapper.parseFrom(bytes)
    new SparkPlanGraphWrapper(
      executionId = wrapper.getExecutionId,
      nodes = wrapper.getNodesList.asScala.map(deserializeSparkPlanGraphNodeWrapper).toSeq,
      edges = wrapper.getEdgesList.asScala.map(deserializeSparkPlanGraphEdge).toSeq
    )
  }

  private def serializeSparkPlanGraphNodeWrapper(input: SparkPlanGraphNodeWrapper):
    StoreTypes.SparkPlanGraphNodeWrapper = {

    val builder = StoreTypes.SparkPlanGraphNodeWrapper.newBuilder()
    builder.setNode(serializeSparkPlanGraphNode(input.node))
    builder.setCluster(serializeSparkPlanGraphClusterWrapper(input.cluster))
    builder.build()
  }

  private def deserializeSparkPlanGraphNodeWrapper(input: StoreTypes.SparkPlanGraphNodeWrapper):
    SparkPlanGraphNodeWrapper = {

    new SparkPlanGraphNodeWrapper(
      node = deserializeSparkPlanGraphNode(input.getNode),
      cluster = deserializeSparkPlanGraphClusterWrapper(input.getCluster)
    )
  }

  private def serializeSparkPlanGraphEdge(edge: SparkPlanGraphEdge):
    StoreTypes.SparkPlanGraphEdge = {
    val builder = StoreTypes.SparkPlanGraphEdge.newBuilder()
    builder.setFromId(edge.fromId)
    builder.setToId(edge.toId)
    builder.build()
  }

  private def deserializeSparkPlanGraphEdge(edge: StoreTypes.SparkPlanGraphEdge):
    SparkPlanGraphEdge = {
    SparkPlanGraphEdge(
      fromId = edge.getFromId,
      toId = edge.getToId)
  }

  private def serializeSparkPlanGraphNode(node: SparkPlanGraphNode):
    StoreTypes.SparkPlanGraphNode = {
    val builder = StoreTypes.SparkPlanGraphNode.newBuilder()
    builder.setId(node.id)
    builder.setName(node.name)
    builder.setDesc(node.desc)
    node.metrics.foreach { metric =>
      builder.addMetrics(SQLPlanMetricSerializer.serialize(metric))
    }
    builder.build()
  }

  private def deserializeSparkPlanGraphNode(node: StoreTypes.SparkPlanGraphNode):
    SparkPlanGraphNode = {

    new SparkPlanGraphNode(
      id = node.getId,
      name = node.getName,
      desc = node.getDesc,
      metrics = node.getMetricsList.asScala.map(SQLPlanMetricSerializer.deserialize).toSeq
    )
  }

  private def serializeSparkPlanGraphClusterWrapper(cluster: SparkPlanGraphClusterWrapper):
    StoreTypes.SparkPlanGraphClusterWrapper = {
    val builder = StoreTypes.SparkPlanGraphClusterWrapper.newBuilder()
    builder.setId(cluster.id)
    builder.setName(cluster.name)
    builder.setDesc(cluster.desc)
    cluster.nodes.foreach { node =>
      builder.addNodes(serializeSparkPlanGraphNodeWrapper(node))
    }
    cluster.metrics.foreach { metric =>
      builder.addMetrics(SQLPlanMetricSerializer.serialize(metric))
    }
    builder.build()
  }

  private def deserializeSparkPlanGraphClusterWrapper(
    cluster: StoreTypes.SparkPlanGraphClusterWrapper): SparkPlanGraphClusterWrapper = {

    new SparkPlanGraphClusterWrapper(
      id = cluster.getId,
      name = cluster.getName,
      desc = cluster.getDesc,
      nodes = cluster.getNodesList.asScala.map(deserializeSparkPlanGraphNodeWrapper).toSeq,
      metrics = cluster.getMetricsList.asScala.map(SQLPlanMetricSerializer.deserialize).toSeq
    )
  }
}
