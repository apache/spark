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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_LOOKUP_JOIN, TreePattern}
import org.apache.spark.sql.connector.read.{ExternalLookupReaderFactory, Scan}

/**
 * A lookup join retained through ordinary expression and operator optimization.
 *
 * @param requiredLookupOutput Lookup-side attributes still required above this join. They are
 *   part of this node's references, which keeps generic column pruning from projecting the lookup
 *   relation.
 */
case class ExternalLookupJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression],
    requiredLookupOutput: Seq[Attribute]) extends BinaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXTERNAL_LOOKUP_JOIN)

  override def output: Seq[Attribute] = joinType match {
    case Inner => left.output ++ right.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case other => throw SparkException.internalError(s"Unexpected external lookup join type $other")
  }

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan): ExternalLookupJoin = copy(left = newLeft, right = newRight)
}

/** A lookup join after its optimized lookup side has been converted to an external operation. */
case class PreparedExternalLookupJoin(
    child: LogicalPlan,
    tableName: String,
    @transient scan: Scan,
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    lookupInput: Seq[Attribute],
    lookupOutput: Seq[Attribute],
    lookupParameters: Seq[Expression],
    nullSafeParameters: Seq[Boolean],
    joinType: JoinType,
    residualCondition: Option[Expression]) extends UnaryNode {

  override def output: Seq[Attribute] = joinType match {
    case Inner => child.output ++ lookupOutput
    case LeftOuter => child.output ++ lookupOutput.map(_.withNullability(true))
    case other => throw SparkException.internalError(s"Unexpected external lookup join type $other")
  }

  override def references: AttributeSet =
    AttributeSet(lookupParameters.flatMap(_.references)) ++
      residualCondition.map(_.references.intersect(child.outputSet))
        .getOrElse(AttributeSet.empty)

  override protected def withNewChildInternal(newChild: LogicalPlan): PreparedExternalLookupJoin =
    copy(child = newChild)

  // Connector descriptions may contain secrets. The physical node renders a redacted description.
  override protected def stringArgs: Iterator[Any] = Iterator(child)
}
