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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate

/**
 * An implementation for [[PartitionPredicate]] that wraps a Catalyst Expression representing a
 * partition filter.
 * <p>
 * Supporting data sources receive these via
 * [[org.apache.spark.sql.connector.read.SupportsPushDownV2Filters#pushPredicates pushPredicates]]
 * and may use them for additional partition filtering.
 */
class PartitionPredicateImpl(
    private val catalystExpression: Expression,
    private val partitionSchema: Seq[AttributeReference])
  extends PartitionPredicate(
    PartitionPredicate.NAME,
    org.apache.spark.sql.connector.expressions.Expression.EMPTY_EXPRESSION) with Logging {

  // Static validation at construction
  if (partitionSchema.isEmpty) {
    throw SparkException.internalError(
      s"Cannot evaluate partition predicate ${catalystExpression.sql}: partition schema is empty")
  }
  private val refNames = catalystExpression.references.map(_.name).toSet
  private val partitionNames = partitionSchema.map(_.name).toSet
  if (!refNames.subsetOf(partitionNames)) {
    throw SparkException.internalError(
      s"Cannot evaluate partition predicate ${catalystExpression.sql}: expression references " +
      s"${refNames.mkString(", ")} not all in partition columns ${partitionNames.mkString(", ")}")
  }

  /** The wrapped partition filter Catalyst Expression. */
  def expression: Expression = catalystExpression

  override def equals(obj: Any): Boolean = obj match {
    case other: PartitionPredicateImpl =>
      catalystExpression.semanticEquals(other.catalystExpression) &&
        partitionSchema == other.partitionSchema
    case _ => false
  }

  override def hashCode(): Int = {
    31 * catalystExpression.semanticHash() + partitionSchema.hashCode()
  }

  override def toString(): String =
    s"PartitionPredicate(${catalystExpression.sql})"

  /** Bound predicate, computed once and reused for all partition rows. */
  private lazy val boundPredicate: InternalRow => Boolean = {
    val boundExpr = catalystExpression.transform {
      case a: AttributeReference =>
        val index = partitionSchema.indexWhere(_.name == a.name)
        BoundReference(index, partitionSchema(index).dataType, nullable = true)
    }
    val predicate = Predicate.createInterpreted(boundExpr)
    predicate.eval(_)
  }

  override def accept(partitionValues: InternalRow): Boolean = {
    if (partitionValues.numFields != partitionSchema.length) {
      logWarning(
        log"Cannot evaluate partition predicate " +
        log"${MDC(LogKeys.EXPR, catalystExpression.sql)}: " +
        log"partition value field count " +
        log"(${MDC(LogKeys.COUNT, partitionValues.numFields)}) does not " +
        log"match schema (${MDC(LogKeys.NUM_PARTITIONS, partitionSchema.length)}), " +
        log"including partition")
      return true
    }

    try {
      boundPredicate(partitionValues)
    } catch {
      case e: Exception =>
        logWarning(
          log"Failed to evaluate partition predicate " +
          log"${MDC(LogKeys.EXPR, catalystExpression.sql)}, including partition",
          e)
        true
    }
  }

  override def referencedPartitionColumnOrdinals(): Array[Int] = {
    val partitionNames = partitionSchema.map(_.name)
    val ordinals = catalystExpression.references.flatMap { ref =>
      val idx = partitionNames.indexWhere(_ == ref.name)
      if (idx >= 0) {
        Some(idx)
      } else {
        None
      }
    }
    ordinals.toArray.sorted.distinct
  }
}
