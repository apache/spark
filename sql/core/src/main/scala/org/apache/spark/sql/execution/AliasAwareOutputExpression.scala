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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery

/**
 * A trait that provides functionality to handle aliases in the `outputExpressions`.
 */
trait AliasAwareOutputExpression extends UnaryExecNode {
  protected def outputExpressions: Seq[NamedExpression]

  private lazy val aliasMap = outputExpressions.collect {
    case a @ Alias(child, _) => child.canonicalized -> a.toAttribute
  }.toMap

  private lazy val (attributeAliases, nonAttributeAliases) = child.collect {
    case a: AliasAwareOutputExpression => a.outputExpressions.collect { case a: Alias => a }
    case r: ReusedExchangeExec => r.getTagValue(ReuseExchangeAndSubquery.ORIGIN_EXCHANGE).map {
      _.collect {
        case a: AliasAwareOutputExpression => a.outputExpressions.collect { case a: Alias => a }
      }.flatten
    }.getOrElse(Nil)
  }.flatten.partition(_.child.isInstanceOf[Attribute])

  private lazy val nonAttributeAliasesMap =
    nonAttributeAliases.map(a => a.toAttribute.canonicalized -> a.child).toMap

  private lazy val attributeAliasesMap =
    attributeAliases.map(a => a.child.canonicalized -> a.toAttribute).toMap

  protected def hasAlias: Boolean = aliasMap.nonEmpty

  protected def extractFromChildren(exp: Expression): Expression = {
    exp.transformDown {
      case a: Attribute => nonAttributeAliasesMap.getOrElse(a.canonicalized, a)
    }.transformDown {
      case a: Attribute => a
      case e: Expression => e.transformDown {
        case a: Attribute => attributeAliasesMap.getOrElse(a.canonicalized, a)
      }
    }
  }

  protected def normalizeExpression(exp: Expression): Expression = {
    exp.transformDown {
      case e: Expression => aliasMap.getOrElse(e.canonicalized, e)
    }
  }
}

/**
 * A trait that handles aliases in the `outputExpressions` to produce `outputPartitioning` that
 * satisfies distribution requirements.
 */
trait AliasAwareOutputPartitioning extends AliasAwareOutputExpression {
  final override def outputPartitioning: Partitioning = {
    val normalizedOutputPartitioning = if (hasAlias) {
      child.outputPartitioning match {
        case e: Expression =>
          val newOutputPartitioning = normalizeExpression(e).asInstanceOf[Partitioning]
          val extractedOutputPartitioning =
            normalizeExpression(extractFromChildren(e)).asInstanceOf[Partitioning]
          PartitioningCollection(Seq(newOutputPartitioning, extractedOutputPartitioning))
        case other => other
      }
    } else {
      child.outputPartitioning
    }

    flattenPartitioning(normalizedOutputPartitioning).filter {
      case hashPartitioning: HashPartitioning => hashPartitioning.references.subsetOf(outputSet)
      case _ => true
    }.distinct match {
      case Seq() => UnknownPartitioning(child.outputPartitioning.numPartitions)
      case Seq(singlePartitioning) => singlePartitioning
      case seqWithMultiplePartitionings => PartitioningCollection(seqWithMultiplePartitionings)
    }
  }

  private def flattenPartitioning(partitioning: Partitioning): Seq[Partitioning] = {
    partitioning match {
      case PartitioningCollection(childPartitionings) =>
        childPartitionings.flatMap(flattenPartitioning)
      case rest =>
        rest +: Nil
    }
  }
}

/**
 * A trait that handles aliases in the `orderingExpressions` to produce `outputOrdering` that
 * satisfies ordering requirements.
 */
trait AliasAwareOutputOrdering extends AliasAwareOutputExpression {
  protected def orderingExpressions: Seq[SortOrder]

  final override def outputOrdering: Seq[SortOrder] = {
    if (hasAlias) {
      orderingExpressions.map { sortOrder =>
        val newSortOrder = normalizeExpression(sortOrder).asInstanceOf[SortOrder]
        val extractedSortOrder =
          normalizeExpression(extractFromChildren(sortOrder)).asInstanceOf[SortOrder]
        newSortOrder.copy(sameOrderExpressions = newSortOrder.sameOrderExpressions :+
          extractedSortOrder.child)
      }
    } else {
      orderingExpressions
    }
  }
}
