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

import java.io.DataOutputStream

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonRDD, PythonWorker}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriterWrapper
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils


/**
 * Python UDF Runner for cogrouped udfs. It sends Arrow bathes from two different DataFrames,
 * groups them in Python, and receive it back in JVM as batches of single DataFrame.
 */
class CoGroupedArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    leftSchema: StructType,
    rightSchema: StructType,
    timeZoneId: String,
    largeVarTypes: Boolean,
    conf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    profiler: Option[String])
  extends BasePythonRunner[
    (Iterator[InternalRow], Iterator[InternalRow]), ColumnarBatch](
    funcs.map(_._1), evalType, argOffsets, jobArtifactUUID, pythonMetrics)
  with BasicPythonArrowOutput {

  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(
      funcs.head._1.funcs.head.pythonExec)

  override val faultHandlerEnabled: Boolean = SQLConf.get.pythonUDFWorkerFaulthandlerEnabled
  override val idleTimeoutSeconds: Long = SQLConf.get.pythonUDFWorkerIdleTimeoutSeconds
  override val killOnIdleTimeout: Boolean = SQLConf.get.pythonUDFWorkerKillOnIdleTimeout
  override val tracebackDumpIntervalSeconds: Long =
    SQLConf.get.pythonUDFWorkerTracebackDumpIntervalSeconds

  override val hideTraceback: Boolean = SQLConf.get.pysparkHideTraceback
  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  private val maxRecordsPerBatch: Int = {
    val v = SQLConf.get.arrowMaxRecordsPerBatch
    if (v > 0) v else Int.MaxValue
  }
  private val maxBytesPerBatch: Long = SQLConf.get.arrowMaxBytesPerBatch

  protected def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[(Iterator[InternalRow], Iterator[InternalRow])],
      partitionIndex: Int,
      context: TaskContext): Writer = {

    new Writer(env, worker, inputIterator, partitionIndex, context) {

      private var nextBatchInLeftGroup: Iterator[InternalRow] = null
      private var nextBatchInRightGroup: Iterator[InternalRow] = null
      private var leftGroupArrowWriter: ArrowWriterWrapper = null
      private var rightGroupArrowWriter: ArrowWriterWrapper = null

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {

        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets, profiler)
      }

      private def writeFooterAndCloseArrowWriter(writer: ArrowWriterWrapper): Unit = {
        Utils.tryWithSafeFinally {
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.streamWriter.end()
        } {
          writer.close()
        }
      }

      /**
       * Writes the input iterator to the Arrow stream. We slice the input iterator into multiple
       * small batches contain at most `arrowMaxRecordsPerBatch` records.
       */
      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        val startData = dataOut.size()
        // We use null checks for nextBatchInLeftGroup and nextBatchInRightGroup as a way
        // to indicate that the iterator has been consumed.
        // This is because the inputIterator can output empty iterators that need to be
        // written to the stream.
        if (nextBatchInLeftGroup == null && nextBatchInRightGroup == null) {
          if (inputIterator.hasNext) {
            // For each cogroup,
            // first send the number of dataframes in each group,
            // then send first df, then send second df.
            // End of data is marked by sending 0.
            dataOut.writeInt(2)
            val (nextLeft, nextRight) = inputIterator.next()
            nextBatchInLeftGroup = nextLeft
            nextBatchInRightGroup = nextRight
          } else {
            // All the input has been consumed.
            dataOut.writeInt(0)
            return false
          }
        }

        var numRowsInBatch: Int = 0
        if (nextBatchInLeftGroup != null) {
          if (leftGroupArrowWriter == null) {
            leftGroupArrowWriter = ArrowWriterWrapper.createAndStartArrowWriter(leftSchema,
              timeZoneId, pythonExec + " (left)", errorOnDuplicatedFieldNames = true,
              largeVarTypes, dataOut, context)
          }
          numRowsInBatch = BatchedPythonArrowInput.writeSizedBatch(
            leftGroupArrowWriter.arrowWriter,
            leftGroupArrowWriter.streamWriter,
            nextBatchInLeftGroup,
            maxBytesPerBatch,
            maxRecordsPerBatch)

          if (!nextBatchInLeftGroup.hasNext) {
            writeFooterAndCloseArrowWriter(leftGroupArrowWriter)
            nextBatchInLeftGroup = null
            leftGroupArrowWriter = null
          }
        } else if (nextBatchInRightGroup != null) {
          if (rightGroupArrowWriter == null) {
            rightGroupArrowWriter = ArrowWriterWrapper.createAndStartArrowWriter(rightSchema,
              timeZoneId, pythonExec + " (right)", errorOnDuplicatedFieldNames = true,
              largeVarTypes, dataOut, context)
          }
          numRowsInBatch = BatchedPythonArrowInput.writeSizedBatch(
            rightGroupArrowWriter.arrowWriter,
            rightGroupArrowWriter.streamWriter,
            nextBatchInRightGroup,
            maxBytesPerBatch,
            maxRecordsPerBatch)
          if (!nextBatchInRightGroup.hasNext) {
            writeFooterAndCloseArrowWriter(rightGroupArrowWriter)
            nextBatchInRightGroup = null
            rightGroupArrowWriter = null
          }
        } else {
          assert(assertion = false, "Either left or right iterator must be non-empty.")
        }

        // With CoGroupedIterator, we can have empty groups for one of the sides if a grouping
        // key exists in one side but not in the other side.
        assert(0 <= numRowsInBatch && numRowsInBatch <= maxRecordsPerBatch, numRowsInBatch)
        val deltaData = dataOut.size() - startData
        pythonMetrics("pythonDataSent") += deltaData
        true
      }
    }
  }
}

