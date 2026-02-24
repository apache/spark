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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate

/**
 * An implementation for [[PartitionPredicate]] that wraps a Catalyst Expression representing a
 * partition filter.
 * <p>
 * Supporting data sources receive these via
 * [[org.apache.spark.sql.connector.read.SupportsPushDownV2Filters#pushPredicates pushPredicates]]
 * and may use them for partition filtering.
 */
class PartitionPredicateImpl(
    private val catalystExpression: Expression,
    private val partitionSchema: Seq[AttributeReference])
  extends PartitionPredicate(
    PartitionPredicate.NAME,
    org.apache.spark.sql.connector.expressions.Expression.EMPTY_EXPRESSION) with Logging {

  /** The wrapped partition filter Catalyst Expression. */
  def expression: Expression = catalystExpression

  override def toString(): String =
    s"PartitionPredicate(${catalystExpression.sql})"

  override def accept(partitionValues: InternalRow): Boolean = {
    // defensive checks
    if (partitionSchema.isEmpty) {
      logWarning(s"Cannot evaluate partition predicate ${catalystExpression.sql}: " +
        s"partition schema is empty, including partition")
      return true
    }
    if (partitionValues.numFields != partitionSchema.length) {
      logWarning(s"Cannot evaluate partition predicate ${catalystExpression.sql}: " +
        s"partition value field count (${partitionValues.numFields}) does not match schema " +
        s"(${partitionSchema.length}), including partition")
      return true
    }
    val refNames = catalystExpression.references.map(_.name).toSet
    val partitionNames = partitionSchema.map(_.name).toSet
    if (!refNames.subsetOf(partitionNames)) {
      logWarning(s"Cannot evaluate partition predicate ${catalystExpression.sql}: " +
        s"expression references ${refNames.mkString(", ")} not all in partition columns " +
        s"${partitionNames.mkString(", ")}, including partition")
      return true
    }

    // evaluate the catalyst partition filter expression
    try {
      val boundExpr = catalystExpression.transform {
        case a: AttributeReference =>
          val index = partitionSchema.indexWhere(_.name == a.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
      }
      val boundPredicate = Predicate.createInterpreted(boundExpr)
      boundPredicate.eval(partitionValues)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to evaluate partition predicate ${catalystExpression.sql}, " +
          s"including partition", e)
        true
    }
  }

  override def referencedPartitionColumnOrdinals(): Array[Int] = Array.empty[Int]
}
