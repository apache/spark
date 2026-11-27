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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, IOException, OutputStream}
import java.util.concurrent.{CountDownLatch, ExecutionException, FutureTask, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.{SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

class GroupedPythonArrowInputSuite extends QueryTest with SharedSparkSession {
  private type Group = (Iterator[InternalRow], Iterator[InternalRow])

  // Exercise the real input writers without launching a Python worker.
  private def inputWriter(
      cogrouped: Boolean,
      groups: Iterator[Group],
      context: TaskContext): DataOutputStream => Boolean = {
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
    if (cogrouped) {
      class Runner extends CoGroupedArrowPythonRunner(
          funcs, PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF, Array(Array(0)), schema, schema,
          "UTC", false, Map.empty, metrics, None, None) {
        def write: DataOutputStream => Boolean =
          newWriter(SparkEnv.get, null, groups, 0, context).writeNextInputToStream
      }
      new Runner().write
    } else {
      class Runner extends ArrowPythonRunner(
          funcs, PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF, Array(Array(0)), schema,
          "UTC", false, Map.empty, metrics, None, None) with GroupedPythonArrowInput {
        def write: DataOutputStream => Boolean =
          newWriter(SparkEnv.get, null, groups.map(_._1), 0, context).writeNextInputToStream
      }
      new Runner().write
    }
  }

  private def taskContext(): TaskContext = {
    val context = TaskContext.empty()
    // Executor SQLConf is read from task properties, including on the helper writer thread.
    spark.sessionState.conf.getAllConfs.foreach { case (key, value) =>
      context.getLocalProperties.setProperty(key, value)
    }
    context
  }

  private def inputAllocators: Set[BufferAllocator] =
    ArrowUtils.rootAllocator.getChildAllocators.asScala.toSet

  private def rows(values: String*): Iterator[InternalRow] =
    values.iterator.map(value => InternalRow(UTF8String.fromString(value)))

  private val sides = Seq("grouped", "cogrouped left", "cogrouped right")

  private def group(side: String, input: Iterator[InternalRow]): Group = {
    if (side == "cogrouped right") (Iterator.empty, input) else (input, Iterator.empty)
  }

  gridTest("SPARK-62416: reject input after completion before consuming any groups")(
      for (cogrouped <- Seq(false, true); beforeRegistration <- Seq(false, true))
        yield (cogrouped, beforeRegistration)) { case (cogrouped, beforeRegistration) =>
    val context = taskContext()
    TaskContext.withTaskContext(context) {
      val before = inputAllocators
      val groups = new Iterator[Group] {
        override def hasNext: Boolean = fail("Input must not be consumed after task completion")
        override def next(): Group = fail("Input must not be consumed after task completion")
      }
      val bytes = new ByteArrayOutputStream()
      try {
        if (beforeRegistration) context.markTaskCompleted(None)
        val write = inputWriter(cogrouped, groups, context)
        context.markTaskCompleted(None)
        intercept[TaskKilledException](write(new DataOutputStream(bytes)))
        assert(bytes.size() == 0)
        assert(inputAllocators == before)
      } finally {
        context.markTaskCompleted(None)
      }
    }
  }

  gridTest("SPARK-62416: defer cleanup during a batch write or stream initialization")(
      for (side <- sides; failWrite <- Seq(false, true); initializing <- Seq(false, true))
        yield (side, failWrite, initializing)) { case (side, failWrite, initializing) =>
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "2") {
      val context = taskContext()
      TaskContext.withTaskContext(context) {
        val before = inputAllocators
        val writing = new CountDownLatch(1)
        val resumeWrite = new CountDownLatch(1)
        def pause(): Unit = {
          writing.countDown()
          assert(resumeWrite.await(30, TimeUnit.SECONDS))
          // Stop a failed regression before it resumes writing into freed Arrow buffers.
          assert((inputAllocators -- before).nonEmpty)
          if (failWrite) throw new IOException("write failure")
        }
        val input = (0 until 3).iterator.map { i =>
          if (i == 1 && !initializing) pause()
          InternalRow(UTF8String.fromString(s"value-$i"))
        }
        val write = inputWriter(side != "grouped", Iterator.single(group(side, input)), context)
        val bytes = new ByteArrayOutputStream()
        var pauseAt = -1
        val output = new DataOutputStream(new OutputStream {
          override def write(b: Int): Unit = {
            if (bytes.size() == pauseAt) {
              pauseAt = -1
              pause()
            }
            bytes.write(b)
          }
        })
        // Finish the empty left stream before exercising the right stream's lifecycle.
        if (side == "cogrouped right") assert(write(output))
        if (initializing) {
          pauseAt = bytes.size() + (if (side == "cogrouped right") 0 else 4)
        }
        val result = new FutureTask[Boolean](() => TaskContext.withTaskContext(context) {
          write(output)
        })
        val writerThread = new Thread(result, "Grouped Arrow input writer")
        val completion = new FutureTask[Unit](() => context.markTaskCompleted(None))
        val completionThread = new Thread(completion, "Grouped Arrow task completion")
        writerThread.start()
        try {
          assert(writing.await(30, TimeUnit.SECONDS))
          val active = inputAllocators -- before
          assert(active.size == 1)
          assert(active.head.getAllocatedMemory > 0)
          val bytesBeforeCompletion = bytes.size()
          completionThread.start()
          completion.get(30, TimeUnit.SECONDS)
          // Completion must return without waiting for input or closing its live buffers.
          assert(context.isCompleted())
          assert(inputAllocators -- before == active)
          assert(active.head.getAllocatedMemory > 0)
          assert(bytes.size() == bytesBeforeCompletion)
          resumeWrite.countDown()
          if (failWrite) {
            val error = intercept[ExecutionException](result.get(30, TimeUnit.SECONDS))
            assert(error.getCause.getMessage == "write failure")
          } else {
            assert(result.get(30, TimeUnit.SECONDS))
          }
          assert(inputAllocators == before)
          val bytesAfterWrite = bytes.size()
          intercept[TaskKilledException](write(output))
          assert(bytes.size() == bytesAfterWrite)
          assert(inputAllocators == before)
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
  }

  gridTest("SPARK-62416: idle completion closes a partially written group without a footer")(
      sides) { side =>
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      val context = taskContext()
      TaskContext.withTaskContext(context) {
        val before = inputAllocators
        val write = inputWriter(side != "grouped",
          Iterator.single(group(side, rows("first", "second"))), context)
        val bytes = new ByteArrayOutputStream()
        val output = new DataOutputStream(bytes)
        try {
          if (side == "cogrouped right") assert(write(output))
          assert(write(output))
          assert((inputAllocators -- before).size == 1)
          val bytesBeforeCompletion = bytes.size()
          context.markTaskCompleted(None)
          assert(inputAllocators == before)
          assert(bytes.size() == bytesBeforeCompletion)
          intercept[TaskKilledException](write(output))
          assert(bytes.size() == bytesBeforeCompletion)
        } finally {
          context.markTaskCompleted(None)
        }
      }
    }
  }

  gridTest("SPARK-62416: preserve group boundaries, empty cogroups and normal EOF")(
      Seq(false, true)) { cogrouped =>
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      val context = taskContext()
      TaskContext.withTaskContext(context) {
        val before = inputAllocators
        val expected = if (cogrouped) {
          Seq(Seq(Seq("a", "b"), Seq("c")), Seq(Seq.empty, Seq("d", "e")),
            Seq(Seq("f"), Seq.empty), Seq(Seq.empty, Seq.empty))
        } else {
          Seq(Seq(Seq("a", "b")), Seq(Seq("c")))
        }
        val groups = expected.iterator.map { streams =>
          (rows(streams.head: _*), rows(streams.drop(1).flatten: _*))
        }
        val write = inputWriter(cogrouped, groups, context)
        val bytes = new ByteArrayOutputStream()
        val output = new DataOutputStream(bytes)
        try {
          while (write(output)) {}
          assert(inputAllocators == before)
          val input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray))
          expected.foreach { streams =>
            assert(input.readInt() == streams.size)
            streams.foreach { values =>
              val reader = new ArrowStreamReader(input, ArrowUtils.rootAllocator)
              val actual = Seq.newBuilder[String]
              var batches = 0
              try {
                while (reader.loadNextBatch()) {
                  batches += 1
                  val root = reader.getVectorSchemaRoot
                  (0 until root.getRowCount).foreach { i =>
                    actual += root.getVector(0).getObject(i).toString
                  }
                }
                assert(actual.result() == values)
                assert(batches == math.max(values.size, 1))
              } finally {
                reader.close()
              }
            }
          }
          assert(input.readInt() == 0)
          assert(input.read() == -1)
          val bytesBeforeCompletion = bytes.size()
          context.markTaskCompleted(None)
          assert(bytes.size() == bytesBeforeCompletion)
          assert(inputAllocators == before)
        } finally {
          context.markTaskCompleted(None)
        }
      }
    }
  }
}
