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

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonRDD, PythonWorker}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow
import org.apache.spark.sql.execution.arrow.{ArrowWriter, ArrowWriterWrapper}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/**
 * A trait that can be mixed-in with [[BasePythonRunner]]. It implements the logic from
 * JVM (an iterator of internal rows + additional data if required) to Python (Arrow).
 */
private[python] trait PythonArrowInput[IN] { self: BasePythonRunner[IN, _] =>
  protected val workerConf: Map[String, String]

  protected val schema: StructType

  protected val timeZoneId: String

  protected val errorOnDuplicatedFieldNames: Boolean

  protected val largeVarTypes: Boolean

  protected def pythonMetrics: Map[String, SQLMetric]

  protected def writeNextBatchToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[IN]): Boolean

  protected def writeUDF(dataOut: DataOutputStream): Unit

  protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    // Write config for the worker as a number of key -> value pairs of strings
    stream.writeInt(workerConf.size)
    for ((k, v) <- workerConf) {
      PythonRDD.writeUTF(k, stream)
      PythonRDD.writeUTF(v, stream)
    }
  }
  private val arrowSchema = ArrowUtils.toArrowSchema(
    schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
  protected val allocator =
    ArrowUtils.rootAllocator.newChildAllocator(s"stdout writer for $pythonExec", 0, Long.MaxValue)
  protected val root = VectorSchemaRoot.create(arrowSchema, allocator)
  protected var writer: ArrowStreamWriter = _

  protected def close(): Unit = {
    Utils.tryWithSafeFinally {
      // end writes footer to the output stream and doesn't clean any resources.
      // It could throw exception if the output stream is closed, so it should be
      // in the try block.
      writer.end()
    } {
      root.close()
      allocator.close()
    }
  }

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        writeUDF(dataOut)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {

        if (writer == null) {
          writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()
        }

        assert(writer != null)
        writeNextBatchToArrowStream(root, writer, dataOut, inputIterator)
      }
    }
  }
}

private[python] trait BasicPythonArrowInput extends PythonArrowInput[Iterator[InternalRow]] {
  self: BasePythonRunner[Iterator[InternalRow], _] =>
  protected val maxRecordsPerBatch: Int = {
    val v = SQLConf.get.arrowMaxRecordsPerBatch
    if (v > 0) v else Int.MaxValue
  }

  val initCapacity = if (SQLConf.get.arrowMaxRecordsPerBatch > 0) {
    Some(maxRecordsPerBatch)
  } else {
    None
  }
  protected val arrowWriter: arrow.ArrowWriter = ArrowWriter.create(root, initCapacity)

  protected val maxBytesPerBatch: Long = SQLConf.get.arrowMaxBytesPerBatch

  protected def writeNextBatchToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[Iterator[InternalRow]]): Boolean = {

    if (inputIterator.hasNext) {
      val startData = dataOut.size()
      val nextBatch = inputIterator.next()

      while (nextBatch.hasNext) {
        arrowWriter.write(nextBatch.next())
      }

      arrowWriter.finish()
      writer.writeBatch()
      arrowWriter.reset()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
      true
    } else {
      super[PythonArrowInput].close()
      false
    }
  }
}


private[python] trait BatchedPythonArrowInput extends BasicPythonArrowInput {
  self: BasePythonRunner[Iterator[InternalRow], _] =>
  // Marker inside the input iterator to indicate the start of the next batch.
  private var nextBatchStart: Iterator[InternalRow] = Iterator.empty

  override protected def writeNextBatchToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[Iterator[InternalRow]]): Boolean = {
    if (!nextBatchStart.hasNext) {
      if (inputIterator.hasNext) {
        nextBatchStart = inputIterator.next()
      }
    }
    if (nextBatchStart.hasNext) {
      val startData = dataOut.size()

      val numRowsInBatch = BatchedPythonArrowInput.writeSizedBatch(
        arrowWriter, writer, nextBatchStart, maxBytesPerBatch, maxRecordsPerBatch)
      assert(0 < numRowsInBatch && numRowsInBatch <= maxRecordsPerBatch, numRowsInBatch)

      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
      true
    } else {
      super[BasicPythonArrowInput].close()
      false
    }
  }
}

private[python] object BatchedPythonArrowInput {
  /**
   * Split a group into smaller Arrow batches within
   * a separate and complete Arrow streaming format in order
   * to work around Arrow 2G limit, see ARROW-4890.
   *
   * The return value is the number of rows in the batch.
   * Each split Arrow batch also does not have mixed grouped. For example:
   *
   *        +------------------------+      +------------------------+      +--------------------
   *        |Group (by k1) v1, v2, v3|      |Group (by k2) v1, v2, v3|      |                 ...
   *        +------------------------+      +------------------------+      +--------------------
   *
   * +------+-----------------+------+------+-----------------+------+------+--------------------
   * |Schema|            Batch| Batch|Schema|            Batch| Batch|Schema|           Batch ...
   * +------+-----------------+------+------+-----------------+------+------+--------------------
   * |    Arrow Streaming Format     |    Arrow Streaming Format     |    Arrow Streaming Form...
   *
   * Here, each (Arrow) batch does not span multiple groups.
   * These (Arrow) batches within each complete Arrow IPC Format are
   * reconstructed into the group back as pandas instances later on the Python worker side.
   */
  def writeSizedBatch(
      arrowWriter: ArrowWriter,
      writer: ArrowStreamWriter,
      rowIter: Iterator[InternalRow],
      maxBytesPerBatch: Long,
      maxRecordsPerBatch: Int): Int = {
    var numRowsInBatch: Int = 0

    def underBatchSizeLimit: Boolean =
      (maxBytesPerBatch == Int.MaxValue) || (arrowWriter.sizeInBytes() < maxBytesPerBatch)

    while (rowIter.hasNext && numRowsInBatch < maxRecordsPerBatch &&
      underBatchSizeLimit) {
      arrowWriter.write(rowIter.next())
      numRowsInBatch += 1
    }
    arrowWriter.finish()
    writer.writeBatch()
    arrowWriter.reset()
    numRowsInBatch
  }
}

/**
 * Enables an optimization that splits each group into the sized batches.
 */
private[python] trait GroupedPythonArrowInput { self: RowInputArrowPythonRunner =>
  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[Iterator[InternalRow]],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {
      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        writeUDF(dataOut)
      }

      var writer: ArrowWriterWrapper = null
      // Marker inside the input iterator to indicate the start of the next batch.
      private var nextBatchStart: Iterator[InternalRow] = Iterator.empty

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        if (!nextBatchStart.hasNext) {
          if (inputIterator.hasNext) {
            dataOut.writeInt(1) // Notify that there is a group to read.
            assert(writer == null || writer.isClosed)
            writer = ArrowWriterWrapper.createAndStartArrowWriter(
              schema, timeZoneId, pythonExec,
              errorOnDuplicatedFieldNames, largeVarTypes, dataOut, context)
            nextBatchStart = inputIterator.next()
          }
        }
        if (nextBatchStart.hasNext) {
          val startData = dataOut.size()
          val numRowsInBatch: Int = BatchedPythonArrowInput.writeSizedBatch(writer.arrowWriter,
            writer.streamWriter, nextBatchStart, maxBytesPerBatch, maxRecordsPerBatch)
          if (!nextBatchStart.hasNext) {
            writer.streamWriter.end()
            // We don't need a try catch block here as the close() method is registered with
            // the TaskCompletionListener.
            writer.close()
          }
          assert(0 < numRowsInBatch && numRowsInBatch <= maxRecordsPerBatch, numRowsInBatch)
          val deltaData = dataOut.size() - startData
          pythonMetrics("pythonDataSent") += deltaData
          true
        } else {
          dataOut.writeInt(0) // End of data is marked by sending 0.
          false
        }
      }
    }
  }
}
