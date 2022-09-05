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

import java.io._
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.arrow.ArrowWriter.createFieldWriter
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * [[ArrowPythonRunner]] with [[org.apache.spark.sql.streaming.GroupState]].
 */
class ArrowPythonRunnerWithState(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    inputSchema: StructType,
    timeZoneId: String,
    workerConf: Map[String, String],
    stateEncoder: ExpressionEncoder[Row],
    keySchema: StructType,
    valueSchema: StructType,
    stateValueSchema: StructType,
    softLimitBytesPerBatch: Long,
    minDataCountForSample: Int,
    softTimeoutMillsPurgeBatch: Long)
  extends BasePythonRunner[
    (UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow]),
    (Iterator[(UnsafeRow, GroupStateImpl[Row])], Iterator[InternalRow])](
    funcs, evalType, argOffsets) {

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  private val workerConfWithRunnerConfs = workerConf +
    (SQLConf.MAP_PANDAS_UDF_WITH_STATE_SOFT_LIMIT_SIZE_PER_BATCH.key ->
      softLimitBytesPerBatch.toString) +
    (SQLConf.MAP_PANDAS_UDF_WITH_STATE_MIN_DATA_COUNT_FOR_SAMPLE.key ->
      minDataCountForSample.toString) +
    (SQLConf.MAP_PANDAS_UDF_WITH_STATE_SOFT_TIMEOUT_PURGE_BATCH.key ->
      softTimeoutMillsPurgeBatch.toString)

  protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    // Write config for the worker as a number of key -> value pairs of strings
    stream.writeInt(workerConfWithRunnerConfs.size)
    for ((k, v) <- workerConfWithRunnerConfs) {
      PythonRDD.writeUTF(k, stream)
      PythonRDD.writeUTF(v, stream)
    }
    PythonRDD.writeUTF(stateValueSchema.json, stream)
  }

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[(UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow])],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new StateWriterThread(env, worker, inputIterator, partitionIndex, context)
  }

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext)
    : Iterator[(Iterator[(UnsafeRow, GroupStateImpl[Row])], Iterator[InternalRow])] = {
    new StateReaderIterator(stream, writerThread, startTime, env, worker, pid,
      releasedOrClosed, context)
  }

  private class StateWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[(UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow])],
      partitionIndex: Int,
      context: TaskContext)
    extends WriterThread(env, worker, inputIterator, partitionIndex, context) {

    import StateWriterThread._

    private val schemaWithState = inputSchema.add("!__state__!", STATE_METADATA_SCHEMA)

    protected override def writeCommand(dataOut: DataOutputStream): Unit = {
      handleMetadataBeforeExec(dataOut)
      PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
    }

    private def buildStateInfoRow(
        keyRow: UnsafeRow,
        groupState: GroupStateImpl[Row],
        startOffset: Int,
        numRows: Int,
        isLastChunk: Boolean): InternalRow = {
      // NOTE: see ArrowPythonRunnerWithState.STATE_METADATA_SCHEMA
      val stateUnderlyingRow = new GenericInternalRow(
        Array[Any](
          UTF8String.fromString(groupState.json()),
          keyRow.getBytes,
          groupState.getOption.map(PythonSQLUtils.toPyRow).orNull,
          startOffset,
          numRows,
          isLastChunk
        )
      )
      new GenericInternalRow(Array[Any](stateUnderlyingRow))
    }

    protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
      // We initialize all columns in data & state metadata for Arrow RecordBatch.
      val arrowSchema = ArrowUtils.toArrowSchema(schemaWithState, timeZoneId)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdout writer for $pythonExec", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)

      Utils.tryWithSafeFinally {
        // We logically group the columns by family and initialize writer separately, since it's
        // lot more easier and probably performant to write the row directly rather than
        // projecting the row to match up with the overall schema.
        // The number of data rows and state metadata rows can be different which seems to matter
        // for Arrow RecordBatch, so we append empty rows to cover it.
        // We always produce at least one data row per grouping key whereas we only produce one
        // state metadata row per grouping key, so we only need to fill up the empty rows in
        // state metadata side.
        val arrowWriterForData = {
          val children = root.getFieldVectors().asScala.dropRight(1).map { vector =>
            vector.allocateNew()
            createFieldWriter(vector)
          }

          new ArrowWriter(root, children.toArray)
        }
        val arrowWriterForState = {
          val children = root.getFieldVectors().asScala.takeRight(1).map { vector =>
            vector.allocateNew()
            createFieldWriter(vector)
          }
          new ArrowWriter(root, children.toArray)
        }

        val writer = new ArrowStreamWriter(root, null, dataOut)
        writer.start()

        // We apply bin-packing the data from multiple groups into one Arrow RecordBatch to
        // gain the performance. In many cases, the amount of data per grouping key is quite
        // small, which does not seem to maximize the benefits of using Arrow.
        //
        // We have to split the record batch down to each group in Python worker to convert the
        // data for group to Pandas, but hopefully, Arrow RecordBatch provides the way to split
        // the range of data and give a view, say, "zero-copy". To help splitting the range for
        // data, we provide the "start offset" and the "number of data" in the state metadata.
        //
        // FIXME: probably need to change this as "hard limit" when addressing scalability. Worth
        //  noting that we may need to break down the data into chunks for a specific group
        //  having "small" number of data, because we also do bin-packing as well. Maybe we could
        //  concatenate these chunks in Python worker (serializer), with some hints e.g.
        //  We can get the information - the number of data in the chunk before reading.
        //
        // Pretty sure we don't bin-pack all groups into a single record batch. We have a soft
        // limit on the size - it's not a hard limit since we allow current group to write all
        // data even it's going to exceed the limit.
        //
        // We perform some basic sampling for data to guess the size of the data very roughly,
        // and simply multiply by the number of data to estimate the size. We extract the size of
        // data from the record batch rather than UnsafeRow, as we don't hold the memory for
        // UnsafeRow once we write to the record batch. If there is a memory bound here, it
        // should come from record batch.
        //
        // In the meanwhile, we don't also want to let the current record batch collect the data
        // indefinitely, since we are pipelining the process between executor and python worker.
        // Python worker won't process any data if executor is not yet finalized a record
        // batch, which defeats the purpose of pipelining. To address this, we also introduce
        // timeout for constructing a record batch. This is a soft limit indeed as same as limit
        // on the size - we allow current group to write all data even it's timed-out.

        // FIXME: Maybe better if we can extract out the batching logic into a separate class.
        var numRowsForCurGroup = 0
        var startOffsetForCurGroup = 0
        var totalNumRowsForBatch = 0
        var totalNumStatesForBatch = 0

        var sampledDataSizePerRow = 0
        var lastBatchPurgedMillis = System.currentTimeMillis()

        def finalizeCurrentArrowBatch(): Unit = {
          val remainingEmptyStateRows = totalNumRowsForBatch - totalNumStatesForBatch
          (0 until remainingEmptyStateRows).foreach { _ =>
            arrowWriterForState.write(EMPTY_STATE_METADATA_ROW)
          }

          arrowWriterForState.finish()
          arrowWriterForData.finish()
          writer.writeBatch()
          arrowWriterForState.reset()
          arrowWriterForData.reset()

          startOffsetForCurGroup = 0
          numRowsForCurGroup = 0
          totalNumRowsForBatch = 0
          totalNumStatesForBatch = 0
          lastBatchPurgedMillis = System.currentTimeMillis()
        }

        while (inputIterator.hasNext) {
          val (keyRow, groupState, dataIter) = inputIterator.next()

          assert(dataIter.hasNext, "should have at least one data row!")

          numRowsForCurGroup = 0

          // Provide data rows
          while (dataIter.hasNext) {
            val dataRow = dataIter.next()
            // TODO: if we think there will be non-small amount of data per grouping key,
            //  we could probably try out "dictionary encoding" for the optimization
            //  of storing same grouping keys multiple times. This may complicate the logic, as
            //  in IPC streaming format, DictionaryBatch will be provided separately along with
            //  RecordBatch, and I'm not sure whether the record batch can be directly converted
            //  to Pandas DataFrame / Series if the record batch refers to the dictionary batch.
            //  https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout
            //  https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
            arrowWriterForData.write(dataRow)
            numRowsForCurGroup += 1
            totalNumRowsForBatch += 1

            // Currently, this only works when the number of rows are greater than the minimum
            // data count for sampling. And we technically have no way to pick some rows from
            // record batch and measure the size of data, hence we leverage all data in current
            // record batch. We only sample once as it could be costly.
            if (sampledDataSizePerRow == 0 && totalNumRowsForBatch > minDataCountForSample) {
              sampledDataSizePerRow = arrowWriterForData.sizeInBytes() / totalNumRowsForBatch
            }

            // If it exceeds the condition of batch (only size, not about timeout) and
            // there is more data for the same group, flush and construct a new batch.

            //  if (sampledDataSizePerRow * totalNumRowsForBatch >= softLimitBytesPerBatch &&
            //      dataIter.hasNext) {
            // FIXME: DEBUGGING now... split the data per 10 elements <- 1 element for testing
            if (numRowsForCurGroup % 10 == 1 && dataIter.hasNext) {
              // Provide state metadata row as intermediate
              val stateInfoRow = buildStateInfoRow(keyRow, groupState, startOffsetForCurGroup,
                numRowsForCurGroup, isLastChunk = false)
              arrowWriterForState.write(stateInfoRow)
              totalNumStatesForBatch += 1

              finalizeCurrentArrowBatch()
            }
          }

          // Provide state metadata row
          val stateInfoRow = buildStateInfoRow(keyRow, groupState, startOffsetForCurGroup,
            numRowsForCurGroup, isLastChunk = true)
          arrowWriterForState.write(stateInfoRow)
          totalNumStatesForBatch += 1

          // The start offset for next group would be same as the total number of rows for batch,
          // unless the next group starts with new batch.
          startOffsetForCurGroup = totalNumRowsForBatch

          // FIXME: Do we need to come up with sampling "across record batches"?
          // FIXME: Do we need to also come up with the size of state metadata as well?
          // FIXME: Do we need to separate the case of "state with value" vs
          //  "state without value" on sampling?

          // Currently, this only works when the number of rows are greater than the minimum
          // data count for sampling. And we technically have no way to pick some rows from
          // record batch and measure the size of data, hence we leverage all data in current
          // record batch. We only sample once as it could be costly.
          if (sampledDataSizePerRow == 0 && totalNumRowsForBatch > minDataCountForSample) {
            sampledDataSizePerRow = arrowWriterForData.sizeInBytes() / totalNumRowsForBatch
          }

          // The soft-limit on size effectively works after the sampling has completed, since we
          // multiply the number of rows by 0 if the sampling is still in progress. The
          // soft-limit on timeout always applies.
          if (sampledDataSizePerRow * totalNumRowsForBatch >= softLimitBytesPerBatch ||
            System.currentTimeMillis() - lastBatchPurgedMillis > softTimeoutMillsPurgeBatch) {
            finalizeCurrentArrowBatch()
          }
        }

        if (numRowsForCurGroup > 0) {
          // We still have some rows in the current record batch. Need to flush them as well.
          finalizeCurrentArrowBatch()
        }

        // end writes footer to the output stream and doesn't clean any resources.
        // It could throw exception if the output stream is closed, so it should be
        // in the try block.
        writer.end()
      } {
        // If we close root and allocator in TaskCompletionListener, there could be a race
        // condition where the writer thread keeps writing to the VectorSchemaRoot while
        // it's being closed by the TaskCompletion listener.
        // Closing root and allocator here is cleaner because root and allocator is owned
        // by the writer thread and is only visible to the writer thread.
        //
        // If the writer thread is interrupted by TaskCompletionListener, it should either
        // (1) in the try block, in which case it will get an InterruptedException when
        // performing io, and goes into the finally block or (2) in the finally block,
        // in which case it will ignore the interruption and close the resources.
        root.close()
        allocator.close()
      }
    }
  }

  object StateWriterThread {
    val STATE_METADATA_SCHEMA: StructType = StructType(
      Array(
        StructField("properties", StringType),
        StructField("keyRowAsUnsafe", BinaryType),
        StructField("object", BinaryType),
        StructField("startOffset", IntegerType),
        StructField("numRows", IntegerType),
        StructField("isLastChunk", BooleanType)
      )
    )

    // To avoid initializing a new row for empty state metadata row.
    val EMPTY_STATE_METADATA_ROW = new GenericInternalRow(
      Array[Any](null, null, null, null, null, null))
  }

  class StateReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext)
    extends ReaderIterator(stream, writerThread, startTime, env, worker, pid,
      releasedOrClosed, context) {

    private val stateRowDeserializer = stateEncoder.createDeserializer()

    private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"stdin reader for $pythonExec", 0, Long.MaxValue)

    private var reader: ArrowStreamReader = _
    private var root: VectorSchemaRoot = _
    private var schema: StructType = _
    private var vectors: Array[ColumnVector] = _

    context.addTaskCompletionListener[Unit] { _ =>
      if (reader != null) {
        reader.close(false)
      }
      allocator.close()
    }

    private var batchLoaded = true

    protected override def read()
      : (Iterator[(UnsafeRow, GroupStateImpl[Row])], Iterator[InternalRow]) = {
      if (writerThread.exception.isDefined) {
        throw writerThread.exception.get
      }
      try {
        if (reader != null && batchLoaded) {
          batchLoaded = reader.loadNextBatch()
          if (batchLoaded) {
            val batch = new ColumnarBatch(vectors)
            batch.setNumRows(root.getRowCount)
            deserializeColumnarBatch(batch)
          } else {
            reader.close(false)
            allocator.close()
            // Reach end of stream. Call `read()` again to read control data.
            read()
          }
        } else {
          stream.readInt() match {
            case SpecialLengths.START_ARROW_STREAM =>
              reader = new ArrowStreamReader(stream, allocator)
              root = reader.getVectorSchemaRoot()
              schema = ArrowUtils.fromArrowSchema(root.getSchema())

              // FIXME: should we validate schema here with value schema?
              // FIXME: should we validate schema here with state metadata schema?

              vectors = root.getFieldVectors().asScala.map { vector =>
                new ArrowColumnVector(vector)
              }.toArray[ColumnVector]
              read()
            case SpecialLengths.TIMING_DATA =>
              handleTimingData()
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              throw handlePythonException()
            case SpecialLengths.END_OF_DATA_SECTION =>
              handleEndOfDataSection()
              null
          }
        }
      } catch handleException
    }

    private def deserializeColumnarBatch(batch: ColumnarBatch)
      : (Iterator[(UnsafeRow, GroupStateImpl[Row])], Iterator[InternalRow]) = {
      // This should at least have one row for state. Also, we ensure that all columns across
      // data and state metadata have same number of rows, which is required by Arrow record
      // batch.
      assert(batch.numRows() > 0)
      assert(schema.length == 2)

      def constructIterForData(batch: ColumnarBatch): Iterator[InternalRow] = {
        //  UDF returns a StructType column in ColumnarBatch, select the children here
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = schema(0).dataType.asInstanceOf[StructType]
          .indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())

        flattenedBatch.rowIterator.asScala.flatMap { row =>
          if (row.isNullAt(0)) {
            // The entire row in record batch seems to be for state metadata.
            None
          } else {
            Some(row)
          }
        }
      }

      def constructIterForState(
          batch: ColumnarBatch): Iterator[(UnsafeRow, GroupStateImpl[Row])] = {
        //  UDF returns a StructType column in ColumnarBatch, select the children here
        val structVector = batch.column(1).asInstanceOf[ArrowColumnVector]

        val outputVectors = schema(1).dataType.asInstanceOf[StructType]
          .indices.map(structVector.getChild)
        val flattenedBatchForState = new ColumnarBatch(outputVectors.toArray)
        flattenedBatchForState.setNumRows(batch.numRows())
        flattenedBatchForState.rowIterator().asScala.flatMap { row =>
          implicit val formats = org.json4s.DefaultFormats

          if (row.isNullAt(0)) {
            // The entire row in record batch seems to be for data.
            None
          } else {
            // Received state metadata does not need schema - this class already knows them.
            // Array(
            //   StructField("properties", StringType),
            //   StructField("keyRowAsUnsafe", BinaryType),
            //   StructField("object", BinaryType),
            // )
            // TODO: Do we want to rely on the column name rather than the ordinal for safety?
            val propertiesAsJson = parse(row.getUTF8String(0).toString)
            val keyRowAsUnsafeAsBinary = row.getBinary(1)
            val keyRowAsUnsafe = new UnsafeRow(keySchema.fields.length)
            keyRowAsUnsafe.pointTo(keyRowAsUnsafeAsBinary, keyRowAsUnsafeAsBinary.length)
            val maybeObjectRow = if (row.isNullAt(2)) {
              None
            } else {
              val pickledStateValue = row.getBinary(2)
              Some(PythonSQLUtils.toJVMRow(pickledStateValue, stateValueSchema,
                stateRowDeserializer))
            }

            Some(keyRowAsUnsafe, GroupStateImpl.fromJson(maybeObjectRow, propertiesAsJson))
          }
        }
      }

      (constructIterForState(batch), constructIterForData(batch))
    }
  }
}
