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
import java.util.concurrent.{CountDownLatch, ExecutionException, FutureTask, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator

import org.apache.spark.{SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.NoTimeout
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.python.streaming.ApplyInPandasWithStatePythonRunner
import org.apache.spark.sql.execution.streaming.operators.stateful.flatmapgroupswithstate.GroupStateImpl
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

class PythonArrowInputSuite extends QueryTest with SharedSparkSession {

  // Exercise the real input writers without launching a Python worker.
  private trait InputWriter[IN] extends PythonArrowInput[IN] { self: BasePythonRunner[IN, _] =>
    def input(rows: Iterator[InternalRow]): Iterator[IN]

    def inputAllocator: BufferAllocator = allocator

    def inputWriter(
        rows: Iterator[InternalRow],
        context: TaskContext): DataOutputStream => Boolean = {
      val inputWriter = newWriter(SparkEnv.get, null, input(rows), 0, context)
      inputWriter.writeNextInputToStream
    }
  }

  private def newRunner(stateful: Boolean): InputWriter[_] = {
    val funcs = Seq((ChainedPythonFunctions(Seq(SimplePythonFunction(
      command = Seq.empty,
      envVars = new java.util.HashMap[String, String](),
      pythonIncludes = List.empty[String].asJava,
      pythonExec = "python3",
      pythonVer = "3",
      broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
      accumulator = null))), 0L))
    val schema = new StructType().add("s", StringType)
    val metrics = Map("pythonDataSent" -> SQLMetrics.createSizeMetric(sparkContext, "data sent"))
    if (stateful) {
      val keySchema = new StructType().add("key", IntegerType)
      val stateSchema = new StructType().add("count", LongType)
      new ApplyInPandasWithStatePythonRunner(
        funcs,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        Array(Array(0)),
        schema,
        "UTC",
        Map.empty,
        ExpressionEncoder(stateSchema).resolveAndBind(),
        keySchema,
        schema,
        stateSchema,
        metrics,
        None) with InputWriter[ApplyInPandasWithStatePythonRunner.InType] {
        override def input(
            rows: Iterator[InternalRow]): Iterator[ApplyInPandasWithStatePythonRunner.InType] = {
          val key = UnsafeProjection.create(keySchema)(InternalRow(0)).copy()
          val state = GroupStateImpl.createForStreaming[Row](
            None, 0L, GroupStateImpl.NO_TIMESTAMP, NoTimeout, hasTimedOut = false,
            watermarkPresent = false)
          Iterator.single((key, state, rows))
        }
      }
    } else {
      new ArrowPythonRunner(
        funcs, PythonEvalType.SQL_SCALAR_PANDAS_UDF, Array(Array(0)), schema, "UTC", false,
        Map.empty, metrics, None, None) with InputWriter[Iterator[InternalRow]] {
        override def input(rows: Iterator[InternalRow]): Iterator[Iterator[InternalRow]] =
          Iterator.single(rows)
      }
    }
  }

  private def inputAllocators: Set[BufferAllocator] =
    ArrowUtils.rootAllocator.getChildAllocators.asScala.toSet

  gridTest("SPARK-60737: task completion before the first input write")(
      for (stateful <- Seq(false, true); completeBeforeRegistration <- Seq(false, true))
        yield (stateful, completeBeforeRegistration)) {
      case (stateful, completeBeforeRegistration) =>
    val context = TaskContext.empty()
    TaskContext.withTaskContext(context) {
      val before = inputAllocators
      val runner = newRunner(stateful)
      val bytes = new ByteArrayOutputStream()
      try {
        if (completeBeforeRegistration) {
          context.markTaskCompleted(None)
        }
        val write = runner.inputWriter(Iterator.empty, context)
        context.markTaskCompleted(None)
        intercept[TaskKilledException] {
          write(new DataOutputStream(bytes))
        }
        assert(bytes.size() == 0)
        assert(inputAllocators == before)
      } finally {
        context.markTaskCompleted(None)
      }
    }
  }

  gridTest("SPARK-60737: defer cleanup until an active input write exits")(
      for (stateful <- Seq(false, true); failInput <- Seq(false, true))
        yield (stateful, failInput)) { case (stateful, failInput) =>
    val context = TaskContext.empty()
    TaskContext.withTaskContext(context) {
      val runner = newRunner(stateful)
      val writing = new CountDownLatch(1)
      val resumeWrite = new CountDownLatch(1)
      val rows = Iterator(0, 1).map { i =>
        if (i == 1) {
          writing.countDown()
          assert(resumeWrite.await(30, TimeUnit.SECONDS))
          // Also check here so a failed regression stops before writing into closed vectors.
          assert(inputAllocators.contains(runner.inputAllocator))
          if (failInput) throw new IllegalStateException("input failure")
        }
        InternalRow(UTF8String.fromString("value"))
      }
      val write = runner.inputWriter(rows, context)
      val bytes = new ByteArrayOutputStream()
      val output = new DataOutputStream(bytes)
      val result = new FutureTask[Boolean](() => TaskContext.withTaskContext(context) {
        write(output)
      })
      val writerThread = new Thread(result, "Arrow input writer")
      val completion = new FutureTask[Unit](() => context.markTaskCompleted(None))
      val completionThread = new Thread(completion, "Arrow input task completion")
      writerThread.start()
      try {
        assert(writing.await(30, TimeUnit.SECONDS))
        val allocator = runner.inputAllocator
        val bytesBeforeCompletion = bytes.size()
        assert(allocator.getAllocatedMemory > 0)
        completionThread.start()
        completion.get(30, TimeUnit.SECONDS)
        // Completion must neither wait for upstream input nor release the active writer's buffers.
        assert(context.isCompleted())
        assert(inputAllocators.contains(allocator))
        assert(allocator.getAllocatedMemory > 0)
        assert(bytes.size() == bytesBeforeCompletion)
        resumeWrite.countDown()
        if (failInput) {
          val error = intercept[ExecutionException](result.get(30, TimeUnit.SECONDS))
          assert(error.getCause.getMessage == "input failure")
        } else {
          assert(result.get(30, TimeUnit.SECONDS))
        }
        assert(!inputAllocators.contains(allocator))
        intercept[TaskKilledException](write(output))
      } finally {
        resumeWrite.countDown()
        writerThread.join(TimeUnit.SECONDS.toMillis(30))
        completionThread.join(TimeUnit.SECONDS.toMillis(30))
        context.markTaskCompleted(None)
        assert(!writerThread.isAlive)
        assert(!completionThread.isAlive)
      }
    }
  }

  gridTest("SPARK-60737: release input buffers at EOF or idle task completion")(
      for (stateful <- Seq(false, true); exhaustInput <- Seq(false, true))
        yield (stateful, exhaustInput)) { case (stateful, exhaustInput) =>
    val context = TaskContext.empty()
    TaskContext.withTaskContext(context) {
      val runner = newRunner(stateful)
      val rows = Iterator.single(InternalRow(UTF8String.fromString("value")))
      val write = runner.inputWriter(rows, context)
      val bytes = new ByteArrayOutputStream()
      val output = new DataOutputStream(bytes)
      try {
        assert(write(output))
        val allocator = runner.inputAllocator
        assert(inputAllocators.contains(allocator))
        if (exhaustInput) {
          assert(!write(output))
          assert(!context.isCompleted())
          assert(!inputAllocators.contains(allocator))
        }
        val bytesBeforeCompletion = bytes.size()
        context.markTaskCompleted(None)
        assert(!inputAllocators.contains(allocator))
        assert(bytes.size() == bytesBeforeCompletion)
        intercept[TaskKilledException](write(output))
      } finally {
        context.markTaskCompleted(None)
      }
    }
  }
}
