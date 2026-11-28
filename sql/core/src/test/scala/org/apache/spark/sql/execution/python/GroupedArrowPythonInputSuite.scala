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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util.Properties

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.TaskCompletionListener

/**
 * Tests the Arrow resources of the input of `GroupedPythonArrowInput` and
 * `CoGroupedArrowPythonRunner`, which write each group as a separate Arrow stream.
 */
class GroupedArrowPythonInputSuite extends QueryTest with SharedSparkSession {

  /** Counts the task completion listeners added to it. */
  private class CountingTaskContext extends TaskContextImpl(
      0, 0, 0, 0, 0, 1, null, new Properties, null, TaskMetrics.empty, 1) {
    var numListeners = 0
    override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
      numListeners += 1
      super.addTaskCompletionListener(listener)
    }
  }

  /** Drives the input side of a runner without a Python worker. */
  private trait InputWriter[IN] { self: BasePythonRunner[IN, _] =>
    def inputWriter(input: Iterator[IN], context: TaskContext): Writer =
      newWriter(SparkEnv.get, null, input, 0, context)
  }

  // Names the allocators of the runners' input, "stdout writer for <pythonExec>".
  private val pythonExec = "GroupedArrowPythonInputSuite"

  private val dataSchema = new StructType().add("key", IntegerType).add("value", StringType)

  private def funcs = Seq((ChainedPythonFunctions(Seq(SimplePythonFunction(
    command = Seq.empty,
    envVars = new java.util.HashMap[String, String](),
    pythonIncludes = List.empty[String].asJava,
    pythonExec = pythonExec,
    pythonVer = "3",
    broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
    accumulator = null))), 0L))

  private def metrics: Map[String, SQLMetric] =
    Map("pythonDataSent" -> SQLMetrics.createSizeMetric(sparkContext, "data sent"))

  private def groupRows(k: Int, numRows: Int): Iterator[InternalRow] =
    Iterator.fill(numRows)(InternalRow(k, UTF8String.fromString("value")))

  private def groupedRunner() =
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3") {
      new ArrowPythonRunner(funcs, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF, Array(Array(0, 1)),
        dataSchema, "UTC", largeVarTypes = false, Map.empty, metrics, None, None)
        with GroupedPythonArrowInput with InputWriter[Iterator[InternalRow]]
    }

  private def cogroupedRunner() =
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3") {
      new CoGroupedArrowPythonRunner(funcs, PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        Array(Array(0, 1, 2, 3)), dataSchema, dataSchema, "UTC", largeVarTypes = false, Map.empty,
        metrics, None, None) with InputWriter[(Iterator[InternalRow], Iterator[InternalRow])]
    }

  /** The names of the open allocators of the runners' input, with repetitions. */
  private def openInputAllocators(): Seq[String] = ArrowUtils.rootAllocator.getChildAllocators
    .asScala.map(_.getName).filter(_.startsWith(s"stdout writer for $pythonExec")).toSeq.sorted

  test("SPARK-62108: grouped input adds one task completion listener, which closes the " +
      "allocator of the group being written") {
    val context = new CountingTaskContext
    try {
      // 3 groups of 5 rows, written in batches of 3 rows, so 2 calls per group: stop after the
      // first batch of the third group.
      val writer = groupedRunner().inputWriter(Iterator.tabulate(3)(k => groupRows(k, 5)), context)
      val out = new DataOutputStream(new ByteArrayOutputStream())
      (0 until 5).foreach(_ => assert(writer.writeNextInputToStream(out)))
      assert(context.numListeners === 1)
      assert(openInputAllocators() === Seq(s"stdout writer for $pythonExec"))
    } finally {
      context.markTaskCompleted(None)
    }
    assert(openInputAllocators().isEmpty)
  }

  test("SPARK-62108: cogrouped input adds one task completion listener, which closes the " +
      "allocator of the cogroup side being written") {
    // 3 cogroups with 5 rows per side, written in batches of 3 rows, so 2 calls per side: stop
    // after the first batch of the left (9 calls) or the right (11 calls) side of the third one.
    Seq(9 -> "left", 11 -> "right").foreach { case (numCalls, side) =>
      val context = new CountingTaskContext
      try {
        val input = Iterator.tabulate(3)(k => (groupRows(k, 5), groupRows(k, 5)))
        val writer = cogroupedRunner().inputWriter(input, context)
        val out = new DataOutputStream(new ByteArrayOutputStream())
        (0 until numCalls).foreach(_ => assert(writer.writeNextInputToStream(out)))
        assert(context.numListeners === 1, side)
        assert(openInputAllocators() === Seq(s"stdout writer for $pythonExec ($side)"))
      } finally {
        context.markTaskCompleted(None)
      }
      assert(openInputAllocators().isEmpty, side)
    }
  }
}
