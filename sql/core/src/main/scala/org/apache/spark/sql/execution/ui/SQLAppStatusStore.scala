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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppStatusPlugin, ElementTrackingStore}
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

/**
 * Provides a view of a KVStore with methods that make it easy to query SQL-specific state. There's
 * no state kept in this class, so it's ok to have multiple instances of it in an application.
 */
private[sql] class SQLAppStatusStore(
    store: KVStore,
    listener: Option[SQLAppStatusListener] = None) {

  def executionsList(): Seq[SQLExecutionUIData] = {
    store.view(classOf[SQLExecutionUIData]).asScala.toSeq
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

/**
 * An AppStatusPlugin for handling the SQL UI and listeners.
 */
private[sql] class SQLAppStatusPlugin extends AppStatusPlugin {

  override def setupListeners(
      conf: SparkConf,
      store: ElementTrackingStore,
      addListenerFn: SparkListener => Unit,
      live: Boolean): Unit = {
    // For live applications, the listener is installed in [[setupUI]]. This also avoids adding
    // the listener when the UI is disabled. Force installation during testing, though.
    if (!live || Utils.isTesting) {
      val listener = new SQLAppStatusListener(conf, store, live, None)
      addListenerFn(listener)
    }
  }

  override def setupUI(ui: SparkUI): Unit = {
    ui.sc match {
      case Some(sc) =>
        // If this is a live application, then install a listener that will enable the SQL
        // tab as soon as there's a SQL event posted to the bus.
        val listener = new SQLAppStatusListener(sc.conf,
          ui.store.store.asInstanceOf[ElementTrackingStore], true, Some(ui))
        sc.listenerBus.addToStatusQueue(listener)

      case _ =>
        // For a replayed application, only add the tab if the store already contains SQL data.
        val sqlStore = new SQLAppStatusStore(ui.store.store)
        if (sqlStore.executionsCount() > 0) {
          new SQLTab(sqlStore, ui)
        }
    }
  }

}

private[sql] class SQLExecutionUIData(
    @KVIndexParam val executionId: Long,
    val description: String,
    val details: String,
    val physicalPlanDescription: String,
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
    val metricValues: Map[Long, String]
    )

private[sql] class SparkPlanGraphWrapper(
    @KVIndexParam val executionId: Long,
    val nodes: Seq[SparkPlanGraphNodeWrapper],
    val edges: Seq[SparkPlanGraphEdge]) {

  def toSparkPlanGraph(): SparkPlanGraph = {
    SparkPlanGraph(nodes.map(_.toSparkPlanGraphNode()), edges)
  }

}

private[sql] class SparkPlanGraphClusterWrapper(
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
private[sql] class SparkPlanGraphNodeWrapper(
    val node: SparkPlanGraphNode,
    val cluster: SparkPlanGraphClusterWrapper) {

  def toSparkPlanGraphNode(): SparkPlanGraphNode = {
    assert(node == null ^ cluster == null, "One and only of of nore or cluster must be set.")
    if (node != null) node else cluster.toSparkPlanGraphCluster()
  }

}

private[sql] case class SQLPlanMetric(
    name: String,
    accumulatorId: Long,
    metricType: String)
