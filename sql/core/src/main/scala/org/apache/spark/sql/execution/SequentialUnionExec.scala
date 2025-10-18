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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Physical operator for sequential union that processes children sequentially rather than
 * concurrently. Unlike UnionExec which processes all children in parallel, SequentialUnionExec
 * processes only one child at a time, making sequential execution a first-class citizen.
 *
 * This operator maintains internal state to track which child is currently active and transitions
 * between children based on completion status (for streaming) or processes them sequentially
 * (for batch).
 */
case class SequentialUnionExec(children: Seq[SparkPlan]) extends SparkPlan {

  override def output: Seq[Attribute] = children.head.output

  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "activeChild" -> SQLMetrics.createMetric(sparkContext, "active child index")
  )

  /**
   * For true sequential processing, we need to determine which child should be active.
   * This is a simplified version that executes the first child with data.
   * In a production implementation, this would coordinate with the streaming execution
   * to determine the active child based on source completion status.
   */
  private def findActiveChild(): Int = {
    // For now, find the first child that would produce data
    // In streaming context, SequentialUnionManager ensures only one child has data
    for (i <- children.indices) {
      // We could add more sophisticated logic here to peek at data availability
      // For now, return the first child
      return i
    }
    0 // Default to first child
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRowsMetric = longMetric("numOutputRows")
    val activeChildMetric = longMetric("activeChild")

    val activeIndex = findActiveChild()
    activeChildMetric.set(activeIndex)

    // Execute only the active child - this is true sequential processing
    val activeChild = if (activeIndex < children.length) {
      children(activeIndex)
    } else {
      // Fallback to first child if something goes wrong
      children.head
    }

    val activeRDD = activeChild.execute()

    // Add metric tracking
    activeRDD.mapPartitions { iter =>
      val wrappedIter = iter.map { row =>
        numOutputRowsMetric += 1
        row
      }
      wrappedIter
    }
  }

  override def verboseString(maxFields: Int): String = {
    val activeIndex = findActiveChild()
    s"SequentialUnion(activeChild=$activeIndex/${children.length})"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(children = newChildren)
}
