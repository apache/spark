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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.TaskContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

// FIXME: javadoc should provide precondition that input must be sorted
// or both required child distribution as well as required child ordering should be presented
// to guarantee input will be sorted
case class UpdatingSessionExec(
    keyExpressions: Seq[Attribute],
    sessionExpression: Attribute,
    optRequiredChildDistribution: Option[Seq[Distribution]],
    optRequiredChildOrdering: Option[Seq[Seq[SortOrder]]],
    child: SparkPlan) extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      val newIter = new UpdatingSessionIterator(iter, keyExpressions, sessionExpression,
        child.output)

      val debugIter = newIter.map { row =>
        val keysProjection = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        val sessionProjection = GenerateUnsafeProjection.generate(
          Seq(sessionExpression), child.output)
        val rowProjection = GenerateUnsafeProjection.generate(child.output, child.output)

        // FIXME: remove
        val debugPartitionId = TaskContext.get().partitionId()

        logWarning(s"DEBUG: partitionId $debugPartitionId - updated session row - keys ${keysProjection(row)}")
        logWarning(s"DEBUG: partitionId $debugPartitionId - updated session row - session ${sessionProjection(row)}")
        logWarning(s"DEBUG: partitionId $debugPartitionId - updated session row - row (proj) ${rowProjection(row)}")
        logWarning(s"DEBUG: partitionId $debugPartitionId - updated session row - row ${row}")

        row
      }

      debugIter
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = optRequiredChildDistribution match {
    case Some(distribution) => distribution
    case None => super.requiredChildDistribution
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = optRequiredChildOrdering match {
    case Some(ordering) => ordering
    case None => super.requiredChildOrdering
  }
}
