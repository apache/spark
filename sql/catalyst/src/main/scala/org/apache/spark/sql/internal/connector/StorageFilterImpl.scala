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

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression => CatalystExpression, Predicate => CatalystPredicate}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{StorageFilter, StorageFilterNotAppliedException}

/**
 * A [[StorageFilter]] wrapping a Catalyst expression, so a scan can apply a predicate it does not
 * have to understand. It mirrors [[PartitionPredicateImpl]], which evaluates a partition key
 * rather than a data row.
 *
 * `attrs` is the layout of the row [[eval]] expects, in that order, and the expression is bound to
 * it. The binding is built on first use, so an instance created on the driver can be evaluated on
 * an executor.
 */
class StorageFilterImpl private (
    private val catalystExpr: CatalystExpression,
    private val attrs: Seq[Attribute])
  extends StorageFilter with Logging {

  // Whether this instance has logged a failure, so a task logs only its first.
  @transient private var loggedFailure = false

  @transient private lazy val boundPredicate: InternalRow => Boolean = {
    val bound = BindReferences.bindReference(catalystExpr, attrs)
    CatalystPredicate.create(bound).eval
  }

  /**
   * Wraps a non-fatal error in [[StorageFilterNotAppliedException]], unless the task is being
   * killed. Each task logs the first one.
   */
  override def eval(row: InternalRow): Boolean = {
    // Outside the `try`, so a failure to build the predicate fails the task.
    val predicate = boundPredicate
    try {
      predicate(row)
    } catch {
      case NonFatal(e) if !StorageFilterImpl.taskKilled =>
        if (!loggedFailure) {
          loggedFailure = true
          // The columns only, since a runtime bloom's literal would print every byte of the bloom.
          logWarning(log"A storage filter over ${MDC(LogKeys.EXPR, references.mkString(", "))} " +
            log"could not be applied, so the reader reads without it.", e)
        }
        throw new StorageFilterNotAppliedException(e)
    }
  }

  @transient override lazy val references: Array[NamedReference] =
    attrs.map(a => FieldReference.column(a.name)).toArray

  override def equals(obj: Any): Boolean = obj match {
    case other: StorageFilterImpl =>
      catalystExpr.semanticEquals(other.catalystExpr) &&
        attrs.map(_.exprId) == other.attrs.map(_.exprId)
    case _ => false
  }

  override def hashCode(): Int = 31 * catalystExpr.semanticHash() + attrs.map(_.exprId).hashCode()

  // Only the columns, since a runtime bloom's literal would print every byte of the bloom.
  override def toString: String = s"StorageFilter(${references.mkString(", ")})"
}

object StorageFilterImpl {

  /**
   * Builds a filter for `catalystExpr`, whose row layout is the attributes it references, ordered
   * by their position in `output` so the layout is stable across calls. The planner offers a scan
   * only filters over its own columns, so a reference outside `output` is an internal error.
   */
  def apply(catalystExpr: CatalystExpression, output: Seq[Attribute]): StorageFilterImpl = {
    val referenced = output.filter(catalystExpr.references.contains)
    if (referenced.length != catalystExpr.references.size) {
      throw SparkException.internalError(
        s"A storage filter over ${catalystExpr.references.mkString("[", ", ", "]")} references " +
          s"attributes outside the scan output ${output.mkString("[", ", ", "]")}")
    }
    new StorageFilterImpl(catalystExpr, referenced)
  }

  /** Whether the running task is being killed, whose error must not be read as "not applied". */
  private def taskKilled: Boolean = {
    val context = TaskContext.get()
    context != null && context.isInterrupted()
  }
}
