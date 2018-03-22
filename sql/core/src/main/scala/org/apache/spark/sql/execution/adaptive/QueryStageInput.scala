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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._

/**
 * QueryStageInput is the leaf node of a QueryStage and is used to hide its child stage. It gets
 * the result of its child stage and serves it as the input of the QueryStage. A QueryStage knows
 * its child stages by collecting all the QueryStageInputs.
 */
abstract class QueryStageInput extends LeafExecNode {

  def childStage: QueryStage

  // Ignore this wrapper for canonicalizing.
  override def doCanonicalize(): SparkPlan = childStage.canonicalized

  // Similar to ReusedExchangeExec, two QueryStageInputs can reference to the same childStage.
  // QueryStageInput can have distinct set of output attribute ids from its childStage, we need
  // to update the attribute ids in outputPartitioning and outputOrdering.
  private lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(childStage.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = childStage.outputPartitioning match {
    case h: HashPartitioning => h.copy(expressions = h.expressions.map(updateAttr))
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    childStage.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false): StringBuilder = {
    childStage.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }
}

/**
 * A QueryStageInput whose child stage is a ShuffleQueryStage. It returns a new ShuffledRowRDD
 * based on the the child stage's result RDD and the specified partitionStartIndices. If the
 * child stage is reused by another ShuffleQueryStageInput, they can return RDDs with different
 * partitionStartIndices.
 */
case class ShuffleQueryStageInput(
    childStage: ShuffleQueryStage,
    override val output: Seq[Attribute],
    partitionStartIndices: Option[Array[Int]] = None)
  extends QueryStageInput {

  override def outputPartitioning: Partitioning = partitionStartIndices.map {
    indices => UnknownPartitioning(indices.length)
  }.getOrElse(super.outputPartitioning)

  override def doExecute(): RDD[InternalRow] = {
    val childRDD = childStage.execute().asInstanceOf[ShuffledRowRDD]
    new ShuffledRowRDD(childRDD.dependency, partitionStartIndices)
  }
}

/** A QueryStageInput whose child stage is a BroadcastQueryStage. */
case class BroadcastQueryStageInput(
    childStage: BroadcastQueryStage,
    override val output: Seq[Attribute])
  extends QueryStageInput {

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    childStage.executeBroadcast()
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastStageInput does not support the execute() code path.")
  }
}
