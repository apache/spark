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

package org.apache.spark.sql.execution.convention

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec}

/**
 * :: DeveloperApi ::
 * The data layouts a [[org.apache.spark.sql.execution.SparkPlan]] is able to output: at most one
 * row type and at most one batch type. [[RowType.None]] / [[BatchType.None]] mean the plan does
 * not support row-based / columnar execution.
 *
 * `batchType != BatchType.None` must be consistent with `supportsColumnar`, and
 * `rowType != RowType.None` with `supportsRowBased`.
 */
@DeveloperApi
final case class Convention(rowType: RowType, batchType: BatchType) {
  def supportsRow: Boolean = rowType != RowType.None
  def supportsBatch: Boolean = batchType != BatchType.None
}

object Convention {
  /** The convention implied by `supportsRowBased` / `supportsColumnar` of a Spark plan. */
  def spark(supportsRowBased: Boolean, supportsColumnar: Boolean): Convention = Convention(
    if (supportsRowBased) RowType.SparkRowType else RowType.None,
    if (supportsColumnar) BatchType.SparkBatchType else BatchType.None)
}

/**
 * :: DeveloperApi ::
 * A row or columnar data layout. Each type is a vertex of a [[TransitionGraph]]; it
 * registers the transitions from / to other types in [[registerTransitions]].
 *
 * Plugins must explicitly register their types with the graph supplied to
 * `ApplyColumnarRulesAndInsertTransitions` before planning.
 * Types referenced by a transition must already be registered.
 */
@DeveloperApi
sealed abstract class ConventionType extends Serializable {
  /** Override to declare transitions from / to this type in the supplied graph. */
  protected[convention] def registerTransitions(implicit graph: TransitionGraph): Unit = {}

  protected final def fromRow(from: RowType, transition: Transition)(
      implicit graph: TransitionGraph): Unit = {
    graph.addEdge(from, this, transition)
  }

  protected final def toRow(to: RowType, transition: Transition)(
      implicit graph: TransitionGraph): Unit = {
    graph.addEdge(this, to, transition)
  }

  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

/**
 * :: DeveloperApi ::
 * A row data layout, i.e. the kind of `InternalRow` a plan produces in `execute()`.
 */
@DeveloperApi
abstract class RowType extends ConventionType

object RowType {
  /** The plan does not support row-based execution. */
  case object None extends RowType

  /** Spark's `InternalRow`. */
  case object SparkRowType extends RowType
}

/**
 * :: DeveloperApi ::
 * A columnar data layout, i.e. the kind of `ColumnarBatch` a plan produces in
 * `executeColumnar()`.
 *
 * Unless a batch type documents otherwise, a batch returned by an iterator is only valid until the
 * next call to `next()`, and it is owned (and released) by its producer. A consumer that needs the
 * data for longer must copy it or transfer its ownership.
 */
@DeveloperApi
abstract class BatchType extends ConventionType

object BatchType {
  /** The plan does not support columnar execution. */
  case object None extends BatchType

  /** A `ColumnarBatch` with any of Spark's built-in `ColumnVector`s. */
  case object SparkBatchType extends BatchType {
    override protected[convention] def registerTransitions(
        implicit graph: TransitionGraph): Unit = {
      fromRow(RowType.SparkRowType, Transition(RowToColumnarExec(_)))
      toRow(RowType.SparkRowType, Transition(ColumnarToRowExec(_)))
    }
  }

  /**
   * A `ColumnarBatch` whose columns are all
   * [[org.apache.spark.sql.vectorized.ArrowColumnVector]]s. Consumers can access the underlying
   * Arrow `FieldVector`s directly, e.g. to serialize them to Arrow IPC without conversion.
   */
  case object ArrowBatchType extends BatchType {
    override protected[convention] def registerTransitions(
        implicit graph: TransitionGraph): Unit = {
      toRow(RowType.SparkRowType, Transition(ColumnarToRowExec(_)))
    }
  }
}
