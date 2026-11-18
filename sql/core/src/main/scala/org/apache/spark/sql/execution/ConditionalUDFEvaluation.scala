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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow,
  JoinedRow, Predicate, UnsafeProjection}
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
 * without starting a UDF worker. Once a row needs the UDF, the rest of the partition is evaluated
 * through `evaluate`, buffering (spilling to disk if needed) the skipped rows between evaluated
 * ones to restore input order. `evaluate` sends rows to the UDF in batches, so a skipped row is
 * held until the batch holding the next row that needs the UDF has been answered; the skipped
 * rows after the last such row are held until the partition's input ends. A partition in which
 * few rows need the UDF therefore buffers most of the rows after the first one.
 *
 * `evaluate` must consume each input row before pulling the next, since some callers do not copy
 * the input rows; this iterator holds the first row that needs the UDF and does not advance the
 * input past it until `evaluate` is pulled.
 */
private[sql] object ConditionalUDFEvaluation {

  /**
   * @param input the child rows, as unsafe rows
   * @param condition decides which rows to evaluate; must be deterministic
   * @param childOutput the attributes of the child rows, which `condition` refers to
   * @param passThroughOutput the child columns passed through to the output (see
   *                          `EvalPythonEvaluatorFactory`); a prefix of the output
   * @param resultOutput the attributes of the UDF results, the rest of the output
   * @param lockFree whether the queue of skipped rows has a single producer and a single
   *                 consumer on different threads (a pipelined Python writer); false when the
   *                 same thread drives `evaluate` and reads its results
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
      lockFree: Boolean)(
      evaluate: Iterator[InternalRow] => Iterator[InternalRow]): Iterator[InternalRow] = {
    val output = passThroughOutput ++ resultOutput
    val predicate = Predicate.create(condition, childOutput)
    predicate.initialize(context.partitionId())
    // Project each skipped child row to the pass-through columns the output keeps, as the
    // evaluator does for the rows it evaluates, then pad it with null results.
    val passThrough = UnsafeProjection.create(passThroughOutput, childOutput)
    passThrough.initialize(context.partitionId())
    val nullResults = new GenericInternalRow(resultOutput.length)
    val joined = new JoinedRow
    val skippedProj = UnsafeProjection.create(output, output)
    skippedProj.initialize(context.partitionId())
    def skippedOut(childRow: InternalRow): InternalRow =
      skippedProj(joined(passThrough(childRow), nullResults))

    // Phase 1: stream the skipped rows before the first row that needs the UDF. `firstEvaluated`
    // holds that row once found, to start phase 2 with it.
    var firstEvaluated: InternalRow = null
    val lead = new Iterator[InternalRow] {
      private var nextOut: InternalRow = null

      override def hasNext: Boolean = {
        if (nextOut != null) return true
        if (firstEvaluated != null) return false
        while (input.hasNext) {
          val row = input.next()
          if (predicate.eval(row)) {
            firstEvaluated = row
            return false
          }
          nextOut = skippedOut(row)
          return true
        }
        false
      }

      override def next(): InternalRow = {
        if (!hasNext) Iterator.empty.next()
        val row = nextOut
        nextOut = null
        row
      }
    }

    // Phase 2: from the first row that needs the UDF onward, evaluate through `evaluate` and
    // interleave the skipped rows back in input order. Built lazily, so a partition that never
    // needs the UDF never reaches it (no buffering, no worker).
    def buffered(): Iterator[InternalRow] = {
      if (firstEvaluated == null) return Iterator.empty
      val remaining = Iterator.single(firstEvaluated) ++ input

      // In pipelined mode the writer thread adds skipped rows (while pulling `evaluated`) and the
      // task thread removes them, one producer and one consumer, so use the lock-free queue (as
      // the Python evaluator does); otherwise the synchronized one.
      val skipped = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        passThroughOutput.length,
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
              skipped.add(passThrough(row))
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
            skippedProj(joined(skipped.remove(), nullResults))
          } else {
            val row = pendingResult
            pendingResult = null
            row
          }
        }
      }
    }

    lead ++ buffered()
  }
}
