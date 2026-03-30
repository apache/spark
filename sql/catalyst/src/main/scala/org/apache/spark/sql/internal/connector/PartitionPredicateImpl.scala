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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression => CatalystExpression, Predicate => CatalystPredicate}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate

/**
 * An implementation for [[PartitionPredicate]] that wraps a Catalyst Expression representing a
 * partition filter.
 */
class PartitionPredicateImpl private (
    private val catalystExpr: CatalystExpression,
    private val partitionSchema: Seq[AttributeReference])
  extends PartitionPredicate with Logging {

  /** The wrapped partition filter Catalyst Expression. */
  def expression: CatalystExpression = catalystExpr

  /** Bound predicate, computed once and reused for all partition rows. */
  @transient private lazy val boundPredicate: InternalRow => Boolean = {
    val boundExpr = catalystExpr.transform {
      case a: AttributeReference =>
        val index = partitionSchema.indexWhere(_.name == a.name)
        require(index >= 0, s"Column ${a.name} not found in partition schema")
        BoundReference(index, partitionSchema(index).dataType, nullable = a.nullable)
    }
    val predicate = CatalystPredicate.createInterpreted(boundExpr)
    predicate.eval
  }

  override def eval(partitionValues: InternalRow): Boolean = {
    if (partitionValues.numFields != partitionSchema.length) {
      logWarning(
        log"Cannot evaluate partition predicate ${MDC(LogKeys.EXPR, catalystExpr.sql)}: " +
        log"partition value field count (${MDC(LogKeys.COUNT, partitionValues.numFields)}) " +
        log"does not match schema (${MDC(LogKeys.NUM_PARTITIONS, partitionSchema.length)}). " +
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
    val refNames = catalystExpr.references.map(_.name).toSet
    partitionSchema.zipWithIndex
      .filter { case (attr, _) => refNames.contains(attr.name) }
      .map { case (attr, ordinal) => PartitionColumnReferenceImpl(ordinal, Array(attr.name)) }
      .toArray
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: PartitionPredicateImpl =>
      catalystExpr.semanticEquals(other.catalystExpr) && partitionSchema == other.partitionSchema
    case _ => false
  }

  override def hashCode(): Int = {
    31 * catalystExpr.semanticHash() + partitionSchema.hashCode()
  }

  override def toString(): String = s"PartitionPredicate(${catalystExpr.sql})"
}

object PartitionPredicateImpl {

  def apply(
      catalystExpr: CatalystExpression,
      partitionSchema: Seq[AttributeReference]): PartitionPredicateImpl = {
    if (partitionSchema.isEmpty) {
      throw SparkException.internalError(
        s"Cannot evaluate partition predicate ${catalystExpr.sql}: partition schema is empty")
    }
    val partitionNames = partitionSchema.map(_.name).toSet
    val refNames = catalystExpr.references.map(_.name).toSet
    if (!refNames.subsetOf(partitionNames)) {
      throw SparkException.internalError(
        s"Cannot evaluate partition predicate ${catalystExpr.sql}: expression references " +
        s"${refNames.mkString(", ")} not all in partition columns ${partitionNames.mkString(", ")}")
    }
    new PartitionPredicateImpl(catalystExpr, partitionSchema)
  }
}
