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

package org.apache.spark.sql.execution.ui

import java.lang.{Long => JLong}
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.KVUtils
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.{KVIndex, KVStore}

/**
 * Provides a view of a KVStore with methods that make it easy to query SQL-specific state. There's
 * no state kept in this class, so it's ok to have multiple instances of it in an application.
 */
class SQLAppStatusStore(
    store: KVStore,
    val listener: Option[SQLAppStatusListener] = None) {

  def executionsList(): Seq[SQLExecutionUIData] = {
    KVUtils.viewToSeq(store.view(classOf[SQLExecutionUIData]))
  }

  def executionsList(offset: Int, length: Int): Seq[SQLExecutionUIData] = {
    KVUtils.viewToSeq(store.view(classOf[SQLExecutionUIData]).skip(offset).max(length))
  }

  def execution(executionId: Long): Option[SQLExecutionUIData] = {
    try {
      Some(store.read(classOf[SQLExecutionUIData], executionId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def executionsCount(): Long = {
    store.count(classOf[SQLExecutionUIData])
  }

  def planGraphCount(): Long = {
    store.count(classOf[SparkPlanGraphWrapper])
  }

  def executionMetrics(executionId: Long): Map[Long, String] = {
    def metricsFromStore(): Option[Map[Long, String]] = {
      val exec = store.read(classOf[SQLExecutionUIData], executionId)
      Option(exec.metricValues)
    }

    metricsFromStore()
      .orElse(listener.flatMap(_.liveExecutionMetrics(executionId)))
      // Try a second time in case the execution finished while this method is trying to
      // get the metrics.
      .orElse(metricsFromStore())
      .getOrElse(Map())
  }

  def planGraph(executionId: Long): SparkPlanGraph = {
    store.read(classOf[SparkPlanGraphWrapper], executionId).toSparkPlanGraph()
  }
}

class SQLExecutionUIData(
    @KVIndexParam val executionId: Long,
    val description: String,
    val details: String,
    val physicalPlanDescription: String,
    val modifiedConfigs: Map[String, String],
    val metrics: Seq[SQLPlanMetric],
    val submissionTime: Long,
    val completionTime: Option[Date],
    @JsonDeserialize(keyAs = classOf[Integer])
    val jobs: Map[Int, JobExecutionStatus],
    @JsonDeserialize(contentAs = classOf[Integer])
    val stages: Set[Int],
    /**
     * This field is only populated after the execution is finished; it will be null while the
     * execution is still running. During execution, aggregate metrics need to be retrieved
     * from the SQL listener instance.
     */
    @JsonDeserialize(keyAs = classOf[JLong])
    val metricValues: Map[Long, String]) {

  @JsonIgnore @KVIndex("completionTime")
  private def completionTimeIndex: Long = completionTime.map(_.getTime).getOrElse(-1L)
}

class SparkPlanGraphWrapper(
    @KVIndexParam val executionId: Long,
    val nodes: Seq[SparkPlanGraphNodeWrapper],
    val edges: Seq[SparkPlanGraphEdge]) {

  def toSparkPlanGraph(): SparkPlanGraph = {
    SparkPlanGraph(nodes.map(_.toSparkPlanGraphNode()), edges)
  }

}

class SparkPlanGraphClusterWrapper(
    val id: Long,
    val name: String,
    val desc: String,
    val nodes: Seq[SparkPlanGraphNodeWrapper],
    val metrics: Seq[SQLPlanMetric]) {

  def toSparkPlanGraphCluster(): SparkPlanGraphCluster = {
    new SparkPlanGraphCluster(id, name, desc,
      new ArrayBuffer() ++ nodes.map(_.toSparkPlanGraphNode()),
      metrics)
  }

}

/** Only one of the values should be set. */
class SparkPlanGraphNodeWrapper(
    val node: SparkPlanGraphNode,
    val cluster: SparkPlanGraphClusterWrapper) {

  def toSparkPlanGraphNode(): SparkPlanGraphNode = {
    assert(node == null ^ cluster == null, "Exactly one of node, cluster values to be set.")
    if (node != null) node else cluster.toSparkPlanGraphCluster()
  }

}

case class SQLPlanMetric(
    name: String,
    accumulatorId: Long,
    metricType: String)
