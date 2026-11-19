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

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, Predicate,
  UnsafeProjection}
import org.apache.spark.sql.execution.python.HybridRowQueue
import org.apache.spark.util.Utils

/**
 * Evaluates UDFs only for the input rows where a deterministic condition is true (see
 * `EvalOnlyWhen`). The rows where it is false are not passed to `evaluate`; they are returned,
 * in their input order among the evaluated rows, with null results (which an enclosing `if`
 * reading the condition does not use).
 *
 * The leading run of skipped rows, before the first row that needs the UDF, is streamed directly
 * and `evaluate` is not called for it, so a partition whose rows all skip the UDF (e.g. every
 * outer row of a COUNT-bug subquery has matching inner rows) streams without buffering and
 * without starting a UDF worker.
 *
 * When `resultIsConstant` is true, the UDFs do not depend on the input row (they are
 * deterministic and their arguments have no references, as for the zero-row result that
 * `DecorrelateInnerQuery` marks, whose aggregates are replaced with their value over zero rows).
 * The first row that needs the UDF is then evaluated once, and its results are reused for every
 * later row that needs the UDF, so the whole partition streams in input order without a queue.
 *
 * Otherwise, from the first row that needs the UDF the rest of the partition is evaluated through
 * `evaluate`, buffering (spilling to disk if needed) the skipped rows between evaluated ones to
 * restore input order. `evaluate` sends rows to the UDF in batches, so a skipped row is held
 * until the batch holding the next row that needs the UDF has been answered; the skipped rows
 * after the last such row are held until the partition's input ends. A partition in which few
 * rows need a row-dependent UDF therefore buffers most of the rows after the first one.
 *
 * `evaluate` must consume each input row before pulling the next, since some callers do not copy
 * the input rows; this iterator holds the first row that needs the UDF and does not advance the
 * input past it until `evaluate` is pulled.
 */
private[sql] object ConditionalUDFEvaluation {

  /**
   * Whether the UDFs' results do not depend on the input row, so the first evaluated row's result
   * can be reused for the rest: every UDF is deterministic and its arguments have no references
   * (as for a marked zero-row result, whose aggregates are replaced with their value over zero
   * rows). A UDF whose argument is the result of another UDF evaluated in a separate node has a
   * reference to that result, so it is not row-independent here.
   */
  def resultIsRowIndependent(udfs: Seq[Expression]): Boolean =
    udfs.forall(u => u.deterministic && u.references.isEmpty)

  /**
   * @param input the child rows, as unsafe rows
   * @param condition decides which rows to evaluate; must be deterministic
   * @param childOutput the attributes of the child rows, which `condition` refers to
   * @param passThroughOutput the child columns passed through to the output (see
   *                          `EvalPythonEvaluatorFactory`); a prefix of the output
   * @param resultOutput the attributes of the UDF results, the rest of the output
   * @param lockFree whether the queue of skipped rows has a single producer and a single
   *                 consumer on different threads (a pipelined Python writer); false when the
   *                 same thread drives `evaluate` and reads its results. Unused when
   *                 `resultIsConstant`, which keeps no queue
   * @param resultIsConstant whether the UDFs' results do not depend on the input row, so the
   *                         first evaluated row's results can be reused for the rest
   * @param evaluate evaluates the UDFs over the given rows and returns, for each of them in
   *                 order, its `passThroughOutput` columns followed by its results
   */
  def apply(
      input: Iterator[InternalRow],
      condition: Expression,
      childOutput: Seq[Attribute],
      passThroughOutput: Seq[Attribute],
      resultOutput: Seq[Attribute],
      context: TaskContext,
      lockFree: Boolean,
      resultIsConstant: Boolean)(
      evaluate: Iterator[InternalRow] => Iterator[InternalRow]): Iterator[InternalRow] = {
    val output = passThroughOutput ++ resultOutput
    val predicate = Predicate.create(condition, childOutput)
    predicate.initialize(context.partitionId())
    // Project a skipped child row straight to the output: its pass-through columns padded with
    // null results, in one projection (the skipped rows are the common path).
    val nullOut = UnsafeProjection.create(
      passThroughOutput ++ resultOutput.map(a => Literal.create(null, a.dataType)), childOutput)
    nullOut.initialize(context.partitionId())

    // Phase 1: stream the skipped rows before the first row that needs the UDF. `firstEvaluated`
    // holds that row once found, to start phase 2 with it.
    var firstEvaluated: InternalRow = null
    val lead = new Iterator[InternalRow] {
      private var nextOut: InternalRow = null

      override def hasNext: Boolean = {
        if (nextOut != null) return true
        if (firstEvaluated != null) return false
        if (input.hasNext) {
          val row = input.next()
          if (predicate.eval(row)) {
            firstEvaluated = row
            false
          } else {
            nextOut = nullOut(row)
            true
          }
        } else {
          false
        }
      }

      override def next(): InternalRow = {
        if (!hasNext) Iterator.empty.next()
        val row = nextOut
        nextOut = null
        row
      }
    }

    // Phase 2a (resultIsConstant): evaluate the first row that needs the UDF once and reuse its
    // results, since they do not depend on the row. Streams in input order with no queue.
    def constantTail(): Iterator[InternalRow] = {
      val firstOut = evaluate(Iterator.single(firstEvaluated))
      if (!firstOut.hasNext) {
        throw SparkException.internalError("A UDF returned no row for a row it was given.")
      }
      // The results are a row-independent suffix of the output; cache them.
      val resultProj = UnsafeProjection.create(resultOutput, output)
      val cachedResults = resultProj(firstOut.next()).copy()
      // Read the evaluator to the end so it finishes as it would after the last row: the Python
      // runner reads accumulator updates and releases its worker, an external session reaches
      // Finish, and a UDF that returned more rows than it was given is caught.
      if (firstOut.hasNext) {
        throw SparkException.internalError("A UDF returned more rows than it was given.")
      }
      // A kept row in one projection: its pass-through columns followed by the cached results.
      val keptOut = UnsafeProjection.create(
        passThroughOutput ++ resultOutput.zipWithIndex.map { case (a, i) =>
          Literal(cachedResults.get(i, a.dataType), a.dataType) },
        childOutput)
      keptOut.initialize(context.partitionId())
      (Iterator.single(firstEvaluated) ++ input).map { row =>
        if (predicate.eval(row)) keptOut(row) else nullOut(row)
      }
    }

    // Phase 2b: from the first row that needs the UDF onward, evaluate through `evaluate` and
    // interleave the skipped rows back in input order, buffering them in a row queue.
    def buffered(): Iterator[InternalRow] = {
      val remaining = Iterator.single(firstEvaluated) ++ input

      // In pipelined mode the writer thread adds skipped rows (while pulling `evaluated`) and the
      // task thread removes them, one producer and one consumer, so use the lock-free queue (as
      // the Python evaluator does); otherwise the synchronized one. The queue stores the whole
      // output row (pass-through columns padded with null results), so no second projection is
      // needed when a skipped row is emitted.
      val skipped = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        output.length,
        lockFree = lockFree)
      context.addTaskCompletionListener[Unit](_ => skipped.close())
      // For each evaluated row, in order, the number of skipped rows right before it.
      val gaps = new ConcurrentLinkedQueue[java.lang.Long]()
      val numSkipped = new AtomicLong(0L)
      @volatile var inputDone = false

      val evaluated = new Iterator[InternalRow] {
        private var nextRow: InternalRow = null
        private var pending = 0L

        override def hasNext: Boolean = {
          while (nextRow == null && remaining.hasNext) {
            val row = remaining.next()
            if (predicate.eval(row)) {
              gaps.add(pending)
              pending = 0L
              nextRow = row
            } else {
              skipped.add(nullOut(row))
              pending += 1
              numSkipped.incrementAndGet()
            }
          }
          if (nextRow == null) inputDone = true
          nextRow != null
        }

        override def next(): InternalRow = {
          if (!hasNext) Iterator.empty.next()
          val row = nextRow
          nextRow = null
          row
        }
      }

      val results = evaluate(evaluated)

      new Iterator[InternalRow] {
        private var skippedToEmit = 0L
        private var numEmittedSkipped = 0L
        private var pendingResult: InternalRow = null
        private var drained = false

        override def hasNext: Boolean = {
          if (skippedToEmit > 0 || pendingResult != null) {
            true
          } else if (drained) {
            false
          } else if (results.hasNext) {
            pendingResult = results.next()
            val gap = gaps.poll()
            if (gap == null) {
              throw SparkException.internalError("A UDF returned more rows than it was given.")
            }
            skippedToEmit = gap
            true
          } else {
            // The evaluated rows are exhausted; emit the skipped rows after the last of them.
            drained = true
            if (!gaps.isEmpty || (!inputDone && evaluated.hasNext)) {
              throw SparkException.internalError("A UDF returned fewer rows than it was given.")
            }
            skippedToEmit = numSkipped.get() - numEmittedSkipped
            skippedToEmit > 0
          }
        }

        override def next(): InternalRow = {
          if (!hasNext) Iterator.empty.next()
          if (skippedToEmit > 0) {
            skippedToEmit -= 1
            numEmittedSkipped += 1
            skipped.remove()
          } else {
            val row = pendingResult
            pendingResult = null
            row
          }
        }
      }
    }

    // Built lazily, so a partition that never needs the UDF never reaches phase 2 (no worker).
    def tail(): Iterator[InternalRow] = {
      if (firstEvaluated == null) Iterator.empty
      else if (resultIsConstant) constantTail()
      else buffered()
    }

    lead ++ tail()
  }
}
