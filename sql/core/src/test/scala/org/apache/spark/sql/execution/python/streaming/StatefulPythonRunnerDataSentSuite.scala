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
package org.apache.spark.sql.execution.python.streaming

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.logical.NoTimeout
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.PythonArrowInput
import org.apache.spark.sql.execution.streaming.operators.stateful.flatmapgroupswithstate.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests the `pythonDataSent` metric of the stateful Python runners, which write all their Arrow
 * batches through the `ArrowStreamWriter` created on the first call of their input writer.
 */
class StatefulPythonRunnerDataSentSuite extends QueryTest with SharedSparkSession {

  /** Drives the input side of a runner without a Python worker. */
  private trait InputWriter[IN] extends PythonArrowInput[IN] { self: BasePythonRunner[IN, _] =>

    /**
     * Writes `input` and returns the number of bytes written after the Arrow schema message.
     * With `newStreamPerCall`, each call gets a new `DataOutputStream` over the same buffer, as
     * `ReaderInputStream` does on every buffer refill. Otherwise, all the calls share one stream,
     * as the pipelined mode does.
     */
    def writeInput(input: Iterator[IN], newStreamPerCall: Boolean): Long = {
      val schemaMessage = new ByteArrayOutputStream()
      MessageSerializer.serialize(
        new WriteChannel(Channels.newChannel(schemaMessage)), root.getSchema)

      val inputWriter = newWriter(SparkEnv.get, null, input, 0, TaskContext.empty())
      val buffer = new ByteArrayOutputStream()
      val sharedStream = new DataOutputStream(buffer)
      var hasInput = true
      while (hasInput) {
        val dataOut = if (newStreamPerCall) new DataOutputStream(buffer) else sharedStream
        hasInput = inputWriter.writeNextInputToStream(dataOut)
      }
      buffer.size() - schemaMessage.size()
    }
  }

  private val keySchema = new StructType().add("key", IntegerType)
  private val dataSchema = keySchema.add("value", StringType)
  private val stateSchema = new StructType().add("count", LongType)

  private def funcs = Seq((ChainedPythonFunctions(Seq(SimplePythonFunction(
    command = Seq.empty,
    envVars = new java.util.HashMap[String, String](),
    pythonIncludes = List.empty[String].asJava,
    pythonExec = "python3",
    pythonVer = "3",
    broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
    accumulator = null))), 0L))

  // 10 groups of 10 rows, which the runners write in Arrow batches of 3 rows.
  private def groups: Iterator[(InternalRow, Iterator[InternalRow])] = Iterator.tabulate(10) { k =>
    InternalRow(k) -> Iterator.tabulate(10)(i => InternalRow(k, UTF8String.fromString(s"v$i" * 50)))
  }

  private def checkDataSent[IN](
      newRunner: Map[String, SQLMetric] => InputWriter[IN],
      input: () => Iterator[IN]): Unit = {
    Seq(true, false).foreach { newStreamPerCall =>
      val metrics = Map("pythonDataSent" -> SQLMetrics.createSizeMetric(sparkContext, "data sent"))
      val runner = withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3") {
        newRunner(metrics)
      }
      val bytesSent = runner.writeInput(input(), newStreamPerCall)
      withClue(s"newStreamPerCall = $newStreamPerCall: ") {
        assert(metrics("pythonDataSent").value === bytesSent)
      }
    }
  }

  test("SPARK-60313: applyInPandasWithState counts all the data sent to Python workers") {
    val keyProjection = UnsafeProjection.create(keySchema)
    checkDataSent[ApplyInPandasWithStatePythonRunner.InType](
      metrics => new ApplyInPandasWithStatePythonRunner(
        funcs,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        Array(Array(0, 1)),
        dataSchema,
        "UTC",
        Map.empty,
        ExpressionEncoder(stateSchema).resolveAndBind(),
        keySchema,
        dataSchema,
        stateSchema,
        metrics,
        None) with InputWriter[ApplyInPandasWithStatePythonRunner.InType],
      () => groups.map { case (key, rows) =>
        val state = GroupStateImpl.createForStreaming[Row](
          None, 0L, GroupStateImpl.NO_TIMESTAMP, NoTimeout, hasTimedOut = false,
          watermarkPresent = false)
        (keyProjection(key).copy(), state, rows)
      })
  }

  test("SPARK-60313: transformWithState counts all the data sent to Python workers") {
    checkDataSent[TransformWithStateInPySparkPythonRunner.InType](
      metrics => new TransformWithStateInPySparkPythonRunner(
        funcs,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF,
        Array(Array(0, 1)),
        dataSchema,
        null,
        "UTC",
        Map.empty,
        metrics,
        None,
        keySchema,
        None,
        None) with InputWriter[TransformWithStateInPySparkPythonRunner.InType],
      () => groups)
  }

  test("SPARK-60313: transformWithState with initial state counts all the data sent to " +
      "Python workers") {
    checkDataSent[TransformWithStateInPySparkPythonRunner.GroupedInType](
      metrics => new TransformWithStateInPySparkPythonInitialStateRunner(
        funcs,
        PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF,
        Array(Array(0, 1)),
        dataSchema,
        dataSchema,
        null,
        "UTC",
        Map.empty,
        metrics,
        None,
        keySchema,
        None,
        None) with InputWriter[TransformWithStateInPySparkPythonRunner.GroupedInType],
      // As in `TransformWithStateInPySparkExec`, the initial state comes before the data.
      () => groups.map((_, true)) ++ groups.map((_, false)))
  }
}
