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

package org.apache.spark.sql.internal.connector

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, Expression => CatalystExpression, ExprId, Predicate => CatalystPredicate}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.types.NullType

/**
 * An implementation for [[PartitionPredicate]] that wraps a Catalyst Expression representing a
 * partition filter.
 *
 * Reporting a partition [[eval]] cannot evaluate as matching only prunes less, which is safe for a
 * runtime filter, whose rows are filtered anyway: by the post-scan `FilterExec` for a scalar
 * subquery filter, by the join it was derived from for a dynamic partition pruning filter, and by
 * the rewrite re-applying its own condition for a row-level operation's group filter. Everywhere
 * else Spark drops a filter the connector accepts, leaving this predicate as the only evaluator,
 * so the failure is propagated instead: reporting a match would return or write rows the filter
 * does not accept.
 *
 * @param catalystExpr the partition filter this predicate evaluates.
 * @param partitionFields one entry per transform of `Table.partitioning()`, in that order, so a
 *                        bound ordinal matches the partition key a connector passes to [[eval]].
 * @param keepOnEvalFailure whether [[eval]] reports a partition it cannot evaluate as matching.
 */
class PartitionPredicateImpl private (
    private val catalystExpr: CatalystExpression,
    private val partitionFields: Seq[PartitionPredicateField],
    private val keepOnEvalFailure: Boolean)
  extends PartitionPredicate with Logging {

  /** Ordinal of each identity partition field, keyed by the attribute a filter references. */
  @transient private lazy val exprIdToIndex: Map[ExprId, Int] =
    partitionFields.zipWithIndex.collect {
      case (PartitionPredicateField(_, Some(attr)), i) => attr.exprId -> i
    }.toMap

  /** The wrapped partition filter Catalyst Expression. */
  def expression: CatalystExpression = catalystExpr

  /** Bound predicate, computed once and reused for all partition rows. */
  @transient private lazy val boundPredicate: InternalRow => Boolean = {
    // One attribute per partition field, so that ordinals match the full partition key. A field
    // of a non-identity transform has no attribute a filter can reference; a placeholder keeps
    // its slot.
    val input = partitionFields.map { f =>
      f.attrRef.getOrElse(AttributeReference(f.fieldNames.mkString("."), NullType)())
    }
    val boundExpr = BindReferences.bindReference(catalystExpr, input)
    val predicate = CatalystPredicate.createInterpreted(boundExpr)
    predicate.eval
  }

  override def eval(partitionValues: InternalRow): Boolean = {
    if (partitionValues.numFields != partitionFields.length) {
      val message =
        log"Cannot evaluate partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}: " +
        log"partition value field count (${MDC(LogKeys.COUNT, partitionValues.numFields)}) " +
        log"does not match schema (${MDC(LogKeys.NUM_PARTITIONS, partitionFields.length)})."
      if (!keepOnEvalFailure) {
        throw SparkException.internalError(message.message)
      }
      logWarning(message + log" Including partition in scan result.")
      return true
    }

    try {
      boundPredicate(partitionValues)
    } catch {
      case e: Exception if keepOnEvalFailure =>
        logWarning(
          log"Failed to evaluate partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}. " +
          log"Including partition in scan result to avoid incorrect filtering.",
          e)
        true
    }
  }

  @transient override lazy val references: Array[NamedReference] = {
    val referencedIndices = catalystExpr.references.flatMap { ref =>
      exprIdToIndex.get(ref.exprId)
    }
    referencedIndices.map { ordinal =>
      PartitionFieldReferenceImpl(ordinal, partitionFields(ordinal).fieldNames)
    }.toArray
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: PartitionPredicateImpl =>
      catalystExpr.semanticEquals(other.catalystExpr) &&
        partitionFields == other.partitionFields &&
        keepOnEvalFailure == other.keepOnEvalFailure
    case _ => false
  }

  override def hashCode(): Int = {
    31 * (31 * catalystExpr.semanticHash() + partitionFields.hashCode()) +
      keepOnEvalFailure.hashCode()
  }

  override def toString(): String = s"PartitionPredicate(${catalystExpr.sql})"
}

object PartitionPredicateImpl extends Logging {

  def apply(catalystExpr: CatalystExpression,
      partitionFields: Seq[PartitionPredicateField],
      keepOnEvalFailure: Boolean = false)
  : Option[PartitionPredicateImpl] = {
    if (partitionFields.isEmpty) {
      logWarning(
        log"Cannot create partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}: " +
        log"partition fields are empty. Skipping pushdown for this predicate.")
      return None
    }

    val partitionExprIds = partitionFields.flatMap(_.attrRef).map(_.exprId).toSet
    val unmatchedRefs = catalystExpr.references.filterNot(r => partitionExprIds.contains(r.exprId))
    if (unmatchedRefs.nonEmpty) {
      logWarning(
        log"Cannot create partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}: " +
        log"expression references " +
        log"${MDC(LogKeys.FIELD_NAME, unmatchedRefs.map(_.name).mkString(", "))} " +
        log"not found in identity partition fields " +
        log"${MDC(LogKeys.PARTITION_SPECIFICATION,
          partitionFields.collect {
            case PartitionPredicateField(names, Some(_)) => names.mkString(".")
          }.mkString(", "))}. " +
        log"Skipping pushdown for this predicate.")
      return None
    }

    Some(new PartitionPredicateImpl(catalystExpr, partitionFields, keepOnEvalFailure))
  }
}
