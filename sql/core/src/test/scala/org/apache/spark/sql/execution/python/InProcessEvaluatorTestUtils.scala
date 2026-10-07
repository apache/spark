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

package org.apache.spark.sql.execution.python

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

/** Fixtures for in-process evaluator tests that need no Python. */
private[python] object InProcessEvaluatorTestUtils {

  /** Every metric that an evaluator may update, as `PythonSQLMetrics` defines them. */
  def allMetrics(): Map[String, SQLMetric] =
    (PythonSQLMetrics.pythonSizeMetricsDesc ++ PythonSQLMetrics.pythonTimingMetricsDesc ++
      PythonSQLMetrics.pythonOtherMetricsDesc).keys.map(_ -> new SQLMetric("sum", 0L)).toMap

  def thread(body: => Unit): Thread = {
    val t = new Thread(() => body)
    t.start()
    t
  }

  /**
   * An evaluator without UDFs over `rowCount` rows of one long column, so that its iterator
   * runs without Python. The input blocks on `gate` before it reads row `blockAt`: in
   * `hasNext`, or in `next` if `blockInNext`.
   */
  class BlockingInput(
      joinInput: InProcessArrowEvalPythonEvaluatorFactory.JoinInput,
      val context: TaskContext,
      session: => InProcessPythonRuntime.InterpreterSession,
      rowCount: Int = Int.MaxValue,
      blockAt: Int = -1,
      blockInNext: Boolean = false,
      batchSize: Int = 10) {
    val reached = new CountDownLatch(1)
    val gate = new CountDownLatch(1)
    val pulled = new AtomicInteger()
    private val column = AttributeReference("x", LongType)()
    private val toUnsafe = UnsafeProjection.create(Array[DataType](LongType))

    private def block(inNext: Boolean): Unit = {
      if (inNext == blockInNext && pulled.get == blockAt) {
        reached.countDown()
        gate.await(10, TimeUnit.SECONDS)
      }
    }

    private val rows: Iterator[InternalRow] = new Iterator[InternalRow] {
      override def hasNext: Boolean = { block(inNext = false); pulled.get < rowCount }
      override def next(): InternalRow = {
        block(inNext = true)
        toUnsafe(InternalRow(pulled.incrementAndGet().toLong)).copy()
      }
    }

    def iterator(): Iterator[InternalRow] = {
      // The byte limit always applies; 64 MB is its default.
      new InProcessArrowEvalPythonEvaluatorFactory(Seq(column), Seq.empty, Seq(column),
          batchSize, 64L << 20, "UTC", false, false, false, false, true, allMetrics()) {
        override private[python] def runtimeSession = session
      }.evaluateBatches(Seq.empty, Array.empty, rows,
        StructType(Seq(StructField("x", LongType))), context, joinInput)
    }

    /** Consumes one row on another thread, returning the thread and its failure. */
    def nextOnAnotherThread(): (Thread, () => Throwable) = {
      val it = iterator()
      @volatile var error: Throwable = null
      val consumer = thread {
        try it.next() catch { case t: Throwable => error = t }
      }
      (consumer, () => error)
    }
  }
}
