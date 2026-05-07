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

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression => CatalystExpression, ExprId, Predicate => CatalystPredicate}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate

/**
 * An implementation for [[PartitionPredicate]] that wraps a Catalyst Expression representing a
 * partition filter.
 */
class PartitionPredicateImpl private (
    private val catalystExpr: CatalystExpression,
    private val partitionFields: Seq[PartitionPredicateField])
  extends PartitionPredicate with Logging {

  @transient private lazy val exprIdToIndex: Map[ExprId, Int] =
    partitionFields.zipWithIndex.map { case (f, i) => f.attrRef.exprId -> i }.toMap

  /** The wrapped partition filter Catalyst Expression. */
  def expression: CatalystExpression = catalystExpr

  /** Bound predicate, computed once and reused for all partition rows. */
  @transient private lazy val boundPredicate: InternalRow => Boolean = {
    val boundExpr = BindReferences.bindReference(catalystExpr, partitionFields.map(_.attrRef))
    val predicate = CatalystPredicate.createInterpreted(boundExpr)
    predicate.eval
  }

  override def eval(partitionValues: InternalRow): Boolean = {
    if (partitionValues.numFields != partitionFields.length) {
      logWarning(
        log"Cannot evaluate partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}: " +
        log"partition value field count (${MDC(LogKeys.COUNT, partitionValues.numFields)}) " +
        log"does not match schema (${MDC(LogKeys.NUM_PARTITIONS, partitionFields.length)}). " +
        log"Including partition in scan result to avoid incorrect filtering.")
      return true
    }

    try {
      boundPredicate(partitionValues)
    } catch {
      case e: Exception =>
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
        partitionFields == other.partitionFields
    case _ => false
  }

  override def hashCode(): Int = {
    31 * catalystExpr.semanticHash() + partitionFields.hashCode()
  }

  override def toString(): String = s"PartitionPredicate(${catalystExpr.sql})"
}

object PartitionPredicateImpl extends Logging {

  def apply(catalystExpr: CatalystExpression,
      partitionFields: Seq[PartitionPredicateField])
  : Option[PartitionPredicateImpl] = {
    if (partitionFields.isEmpty) {
      logWarning(
        log"Cannot create partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}: " +
        log"partition fields are empty. Skipping pushdown for this predicate.")
      return None
    }

    val partitionExprIds = partitionFields.map(_.attrRef.exprId).toSet
    val unmatchedRefs = catalystExpr.references.filterNot(r => partitionExprIds.contains(r.exprId))
    if (unmatchedRefs.nonEmpty) {
      logWarning(
        log"Cannot create partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}: " +
        log"expression references " +
        log"${MDC(LogKeys.FIELD_NAME, unmatchedRefs.map(_.name).mkString(", "))} " +
        log"not found in partition fields " +
        log"${MDC(LogKeys.PARTITION_SPECIFICATION,
          partitionFields.map(_.fieldNames.mkString(".")).mkString(", "))}. " +
        log"Skipping pushdown for this predicate.")
      return None
    }

    Some(new PartitionPredicateImpl(catalystExpr, partitionFields))
  }
}
