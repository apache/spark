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

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests the `pythonDataSent` metric of `GroupedPythonArrowInput` and `CoGroupedArrowPythonRunner`,
 * which create a per-group `ArrowStreamWriter` on the first batch of each group.
 */
class GroupedPythonRunnerDataSentSuite extends QueryTest with SharedSparkSession {

  /** Drives the input side of a runner without a Python worker. */
  private trait InputWriter[IN] { self: BasePythonRunner[IN, _] =>

    /**
     * Writes `input` and returns the total number of bytes written to the stream.
     * With `newStreamPerCall`, each call gets a new `DataOutputStream` over the same buffer, as
     * `ReaderInputStream` and `PipelinedWriterRunnable` do on buffer refills. Otherwise, all calls
     * share one stream.
     */
    def writeInput(input: Iterator[IN], newStreamPerCall: Boolean): Long = {
      val context = TaskContext.empty()
      val inputWriter = newWriter(SparkEnv.get, null, input, 0, context)
      val buffer = new ByteArrayOutputStream()
      val sharedStream = new DataOutputStream(buffer)
      var hasInput = true
      while (hasInput) {
        val dataOut = if (newStreamPerCall) new DataOutputStream(buffer) else sharedStream
        hasInput = inputWriter.writeNextInputToStream(dataOut)
      }
      buffer.size()
    }
  }

  private val dataSchema = new StructType().add("key", IntegerType).add("value", StringType)

  private def funcs = Seq((ChainedPythonFunctions(Seq(SimplePythonFunction(
    command = Seq.empty,
    envVars = new java.util.HashMap[String, String](),
    pythonIncludes = List.empty[String].asJava,
    pythonExec = "python3",
    pythonVer = "3",
    broadcastVars = List.empty[Broadcast[PythonBroadcast]].asJava,
    accumulator = null))), 0L))

  private def groupRows(k: Int, numRows: Int): Iterator[InternalRow] = {
    Iterator.tabulate(numRows)(i => InternalRow(k, UTF8String.fromString(s"v$i" * 50)))
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

  test("GroupedPythonArrowInput counts all the data sent to Python workers") {
    def createRunner(metrics: Map[String, SQLMetric]) =
      new ArrowPythonRunner(
        funcs,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        Array(Array(0, 1)),
        dataSchema,
        "UTC",
        largeVarTypes = false,
        Map.empty,
        metrics,
        None,
        None) with GroupedPythonArrowInput with InputWriter[Iterator[InternalRow]]

    // 10 groups of 10 rows, which the runner writes in Arrow batches of 3 rows.
    checkDataSent[Iterator[InternalRow]](
      createRunner,
      () => Iterator.tabulate(10)(k => groupRows(k, 10)))

    // Empty partition (writes only the 4-byte end-of-data marker).
    checkDataSent[Iterator[InternalRow]](
      createRunner,
      () => Iterator.empty)
  }

  test("CoGroupedArrowPythonRunner counts all the data sent to Python workers") {
    def createRunner(metrics: Map[String, SQLMetric]) =
      new CoGroupedArrowPythonRunner(
        funcs,
        PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        Array(Array(0, 1, 2, 3)),
        dataSchema,
        dataSchema,
        "UTC",
        largeVarTypes = false,
        Map.empty,
        metrics,
        None,
        None) with InputWriter[(Iterator[InternalRow], Iterator[InternalRow])]

    // Includes multi-batch groups on both sides as well as empty left/right groups.
    checkDataSent[(Iterator[InternalRow], Iterator[InternalRow])](
      createRunner,
      () => Iterator.tabulate(10) {
        case 0 => (Iterator.empty, groupRows(0, 10))
        case 1 => (groupRows(1, 10), Iterator.empty)
        case k => (groupRows(k, 10), groupRows(k, 10))
      })

    // Empty partition (writes only the 4-byte end-of-data marker).
    checkDataSent[(Iterator[InternalRow], Iterator[InternalRow])](
      createRunner,
      () => Iterator.empty)
  }
}
