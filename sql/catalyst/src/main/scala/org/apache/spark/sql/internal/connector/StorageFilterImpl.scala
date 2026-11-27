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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BasePredicate, BindReferences, Expression => CatalystExpression}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.{StorageFilter, StorageFilterNotAppliedException}

/**
 * A [[StorageFilter]] wrapping a Catalyst expression, so a scan can apply a predicate it does not
 * have to understand. It mirrors [[PartitionPredicateImpl]], which evaluates a partition key
 * rather than a data row.
 *
 * `referenceArray` is the layout of the row [[eval]] expects, in that order, and the predicate is
 * bound to it. The predicate is built on first use, see [[StorageFilterPredicate]].
 */
class StorageFilterImpl private (
    private val predicate: StorageFilterPredicate,
    private val referenceArray: Array[NamedReference])
  extends StorageFilter with Logging {

  // The width of the row `eval` takes, read on every row.
  private val numReferences = referenceArray.length

  // The built predicate, kept in a plain field, since resolving `predicate.prepared` on every row
  // is a volatile read.
  @transient private var prepared: BasePredicate = _

  // Whether this instance has logged a failure, so a task logs only its first.
  @transient private var loggedFailure = false

  /**
   * Wraps an error in [[StorageFilterNotAppliedException]], unless
   * [[StorageFilterErrors.rethrowIfMustPropagate]] rethrows it, as the file-source reader does: a
   * task kill, or a fatal error in its cause chain. Each task logs the first one. A row of another
   * width than [[references]] is a reader bug, which fails the task, since the predicate would read
   * the wrong values.
   */
  @throws[StorageFilterNotAppliedException]
  override def eval(row: InternalRow): Boolean = {
    // Outside the `try`, so a failure to build the predicate, or a row of another width, fails the
    // task.
    if (row.numFields != numReferences) {
      throw SparkException.internalError(
        s"$this takes rows of $numReferences values, but got ${row.numFields}")
    }
    if (prepared == null) prepared = predicate.prepared
    try {
      prepared.eval(row)
    } catch {
      case e: Throwable =>
        StorageFilterErrors.rethrowIfMustPropagate(e, fromRead = false)
        if (!loggedFailure) {
          loggedFailure = true
          logWarning(log"${MDC(LogKeys.EXPR, this)} could not be applied, so the reader reads " +
            log"without it.", e)
        }
        throw new StorageFilterNotAppliedException(e)
    }
  }

  // A copy per call, so a reader that reorders it cannot change the layout another reader of the
  // task sees.
  override def references(): Array[NamedReference] = referenceArray.clone()

  override def toString: String = StorageFilterImpl.nameOf(referenceArray)
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
    val references = referenced.map(a => FieldReference.column(a.name)).toArray[NamedReference]
    new StorageFilterImpl(
      new StorageFilterPredicate(
        BindReferences.bindReference(catalystExpr, referenced), nameOf(references)),
      references)
  }

  // Only the columns, since a runtime bloom's literal would print every byte of the bloom.
  private def nameOf(references: Array[NamedReference]): String =
    s"StorageFilter(${references.mkString(", ")})"
}
