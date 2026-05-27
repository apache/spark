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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.ArrayImplicits._

/**
 * A mixin for Spark plan nodes that expose driver-side custom metrics reported by a connector.
 * Implementations declare the connector-owned metrics via [[customMetrics]]; after the underlying
 * operation has executed they call [[postDriverMetrics]] with the connector's reported values so
 * they are visible in the SQL UI.
 *
 * Nodes that also expose Spark-owned metrics supply them via [[sparkMetrics]]. Names in
 * [[sparkMetrics]] are reserved: if the connector happens to report a value under the same name,
 * Spark's value wins and the connector's is dropped.
 */
trait SupportsCustomDriverMetrics { self: SparkPlan =>

  /**
   * The custom metrics the connector supports for this operation, keyed by name.
   */
  def customMetrics: Map[String, SQLMetric]

  /**
   * Spark-owned metrics that should appear alongside the connector-declared ones. Values under
   * these names are owned by Spark and take precedence on a name collision.
   */
  protected def sparkMetrics: Map[String, SQLMetric] = Map.empty

  override lazy val metrics: Map[String, SQLMetric] = customMetrics ++ sparkMetrics

  /**
   * Converts an array of connector-declared metrics into the map shape [[customMetrics]] uses.
   */
  protected def createCustomMetrics(metrics: Array[CustomMetric]): Map[String, SQLMetric] = {
    metrics.map { m =>
      m.name -> SQLMetrics.createV2CustomMetric(sparkContext, m)
    }.toMap
  }

  /**
   * Applies the values reported by the connector to the declared metrics and posts them so the
   * SQL UI reflects the final values. Metrics not declared via [[customMetrics]] are ignored.
   * Metrics whose name collides with [[sparkMetrics]] are also ignored so Spark-owned values
   * are preserved.
   */
  protected def postDriverMetrics(taskMetrics: Array[CustomTaskMetric]): Unit = {
    val updated = taskMetrics.flatMap { t =>
      if (sparkMetrics.contains(t.name())) {
        // Spark metrics take precedence on collisions.
        None
      } else {
        metrics.get(t.name()).map { metric =>
          metric.set(t.value())
          metric
        }
      }
    }
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, updated.toImmutableArraySeq)
  }
}
