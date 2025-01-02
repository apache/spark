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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, RowOrdering, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}


/**
 * The cluster spec holds the information of clusters and sorts.
 *
 * @param clusters The clustering expressions
 * @param sorts    The sort orders
 */
case class ClusterSpec(clusters: Seq[Expression], sorts: Seq[SortOrder]) {
  def expressions: Seq[Expression] = clusters ++ sorts.map(_.child)
}

/**
 * Clustering data within the partition and sorting data within the cluster.
 *
 * @param clusterSpec The cluster specification
 * @param child       Child logical plan
 */
case class Clustering(clusterSpec: ClusterSpec, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def outputOrdering: Seq[SortOrder] = clusterSpec.sorts
  override protected def withNewChildInternal(newChild: LogicalPlan): Clustering =
    copy(child = newChild)
}

case class ClusteringExec(
    clusterSpec: ClusterSpec,
    child: SparkPlan) extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val boundClusters = BindReferences.bindReferences(clusterSpec.clusters, output)

    child.execute().mapPartitions { iter =>
      val grouped = mutable.HashMap.empty[Seq[Any], mutable.Buffer[InternalRow]]

      iter.foreach { row =>
        val key = boundClusters.map(_.eval(row))
        grouped.getOrElseUpdate(key, mutable.Buffer()) += row.copy()
      }

      grouped.iterator.flatMap { case (_, rows) =>
        val ordering = RowOrdering.create(clusterSpec.sorts, output)
        rows.sortWith(ordering.compare(_, _) < 0).iterator
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
