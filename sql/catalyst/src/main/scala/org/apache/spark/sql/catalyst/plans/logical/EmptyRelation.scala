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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.trees.TreePattern.{EMPTY_RELATION, TreePattern}

case class EmptyRelation(logical: LogicalPlan) extends LeafNode {
  override val nodePatterns: Seq[TreePattern] = Seq(EMPTY_RELATION)

  override protected def stringArgs: Iterator[Any] = Iterator.empty

  override def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      printOutputColumns: Boolean,
      indent: Int = 0): Unit = {
    super.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      printOutputColumns,
      indent)
    // Nested logical operators are not registered in QueryPlan.localIdMap with physical ids.
    Option(logical).foreach { _ =>
      lastChildren.add(true)
      logical.generateTreeString(
        depth + 1,
        lastChildren,
        append,
        verbose,
        prefix = "",
        addSuffix = false,
        maxFields,
        printNodeId = false,
        printOutputColumns,
        indent)
      lastChildren.remove(lastChildren.size() - 1)
    }
  }

  override val isStreaming: Boolean = logical.isStreaming

  override val outputOrdering: Seq[SortOrder] = logical.outputOrdering

  override def output: Seq[Attribute] = logical.output

  override def computeStats(): Statistics = Statistics(sizeInBytes = 0, rowCount = Some(0))

  override def maxRows: Option[Long] = Some(0)

  override def maxRowsPerPartition: Option[Long] = Some(0)
}
